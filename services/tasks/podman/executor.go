package podman

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/semaphoreui/semaphore/db"
	"github.com/semaphoreui/semaphore/db_lib"
	"github.com/semaphoreui/semaphore/pkg/task_logger"
	"github.com/semaphoreui/semaphore/services/tasks"
	"github.com/semaphoreui/semaphore/util"
)

// Executor implements tasks.Executor by running each task inside an ephemeral
// Podman container. The lifecycle matches LocalExecutor:
//
//  1. Prepare  — clone/pull repository, write inventory + key files to a tmp dir
//  2. Run      — pull image (if needed), start container, stream stdout/stderr
//  3. Cleanup  — remove tmp files and key installations
//
// The container is started with:
//   - the repository working copy mounted read-only at /runner/project
//   - the inventory file mounted read-only at /runner/inventory
//   - credentials passed as env vars (never written to disk inside the container)
//   - a non-root UID enforced via --user
type Executor struct {
	Task        db.Task
	Template    db.Template
	Inventory   db.Inventory
	Repository  db.Repository
	Environment db.Environment
	JWT         string

	cfg          Config
	KeyInstaller db_lib.AccessKeyInstaller
	RepoLock     *tasks.KeyLock
	Logger       task_logger.Logger

	// Kill coordination
	mu                   sync.Mutex
	terminationRequested bool
	cancelFn             context.CancelFunc

	// Paths written by Prepare, cleaned up by Cleanup
	inventoryFilePath string
	tmpDir            string

	prepared bool
}

// --- tasks.Job interface ---

func (e *Executor) Async() bool { return false }

func (e *Executor) IsKilled() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.terminationRequested
}

func (e *Executor) Kill() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.terminationRequested {
		return
	}
	e.terminationRequested = true
	if e.cancelFn != nil {
		e.cancelFn()
	}
}

// --- tasks.Executor interface ---

// SetLogger wires the task log sink into the executor.
func (e *Executor) SetLogger(logger task_logger.Logger) {
	e.Logger = logger
}

func (e *Executor) SetStatus(status task_logger.TaskStatus) {
	if e.Logger != nil {
		e.Logger.SetStatus(status)
	}
}

func (e *Executor) SetCommit(hash, message string) {
	e.Task.CommitHash = &hash
	e.Task.CommitMessage = message
	if e.Logger != nil {
		e.Logger.SetCommit(hash, message)
	}
}

func (e *Executor) Log(msg string) {
	if e.Logger != nil {
		e.Logger.Log(msg)
	}
}

// Prepare materialises everything the container will need:
//   - repository clone/pull + checkout
//   - inventory file written to tmpDir
//   - SSH/vault keys installed to tmpDir
//
// Calling Prepare twice is a no-op (guarded by e.prepared).
func (e *Executor) Prepare(username string, incomingVersion *string, alias string) error {
	if e.prepared {
		return nil
	}

	e.Log(fmt.Sprintf("[UniFlow/podman] Preparing task %d", e.Task.ID))

	// Create a per-task tmp directory under Semaphore's project tmp path.
	tmpDir, err := createTaskTmpDir(util.Config.GetProjectTmpDir(e.Template.ProjectID), e.Task.ID)
	if err != nil {
		e.Log("Failed to create tmp dir: " + err.Error())
		return err
	}
	e.tmpDir = tmpDir

	// Clone or pull the repository.
	if err := e.updateRepository(); err != nil {
		return err
	}

	// Write inventory file.
	inventoryPath, err := writeInventoryFile(e.tmpDir, e.Inventory, e.KeyInstaller, e.Logger)
	if err != nil {
		e.Log("Failed to write inventory: " + err.Error())
		return err
	}
	e.inventoryFilePath = inventoryPath

	e.prepared = true
	return nil
}

// Run pulls the EE image and executes the task inside the container.
// It streams stdout/stderr line-by-line to the Logger so the UI receives
// live output — identical to the LocalExecutor experience.
func (e *Executor) Run(username string, incomingVersion *string, alias string) error {
	if err := e.Prepare(username, incomingVersion, alias); err != nil {
		return err
	}

	image := e.resolveImage()
	if image == "" {
		return fmt.Errorf("no execution environment image configured for this template")
	}

	e.Log(fmt.Sprintf("[UniFlow/podman] Pulling image %s (policy: %s)", image, e.resolvePullPolicy()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(e.cfg.PullTimeoutSeconds)*time.Second)
	e.mu.Lock()
	e.cancelFn = cancel
	e.mu.Unlock()
	defer cancel()

	if err := e.pullImage(ctx, image); err != nil {
		e.Log("[UniFlow/podman] Image pull failed: " + err.Error())
		return err
	}

	e.Log(fmt.Sprintf("[UniFlow/podman] Starting container from %s", image))

	return e.runContainer(ctx, image, username, incomingVersion, alias)
}

// Cleanup removes the tmp directory and any key files written by Prepare.
// Safe to call even when Prepare failed partway through.
func (e *Executor) Cleanup() {
	if e.tmpDir != "" {
		cleanTaskTmpDir(e.tmpDir, e.Logger)
	}
}

// --- internal helpers ---

// resolveImage returns the OCI image reference for this task.
// The image is read from the Template's extra configuration field where
// the UniFlow EE selector stores it (field: uniflow_ee_image).
// Falls back to the UNIFLOW_DEFAULT_EE_IMAGE environment variable.
func (e *Executor) resolveImage() string {
	// Phase 2: read from Template.App or a dedicated EE DB field.
	// For now, read from Environment extra vars as a bootstrap mechanism.
	if img := getEnvVar(e.Environment, "UNIFLOW_EE_IMAGE"); img != "" {
		return img
	}
	// Fallback: check OS environment variable
	return os.Getenv("UNIFLOW_DEFAULT_EE_IMAGE")
}

// resolvePullPolicy returns the pull policy for the current template.
// Supported values: Always, IfNotPresent, Never. Defaults to IfNotPresent.
func (e *Executor) resolvePullPolicy() string {
	policy := getEnvVar(e.Environment, "UNIFLOW_EE_PULL_POLICY")
	switch policy {
	case "Always", "IfNotPresent", "Never":
		return policy
	default:
		return "IfNotPresent"
	}
}

// pullImage runs `podman pull <image>` respecting the pull policy.
func (e *Executor) pullImage(ctx context.Context, image string) error {
	policy := e.resolvePullPolicy()

	if policy == "Never" {
		e.Log("[UniFlow/podman] Pull policy is Never — skipping pull")
		return nil
	}

	if policy == "IfNotPresent" {
		exists, err := imageExists(ctx, image)
		if err != nil {
			return fmt.Errorf("checking local image: %w", err)
		}
		if exists {
			e.Log("[UniFlow/podman] Image already present locally — skipping pull")
			return nil
		}
	}

	cmd := exec.CommandContext(ctx, "podman", "pull", image)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("podman pull %s: %w\n%s", image, err, string(out))
	}
	e.Log("[UniFlow/podman] Pull complete")
	return nil
}

// runContainer starts `podman run` and streams output to the Logger.
func (e *Executor) runContainer(ctx context.Context, image, username string, incomingVersion *string, alias string) error {
	args := e.buildPodmanArgs(image)

	cmd := exec.CommandContext(ctx, "podman", args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("podman run: %w", err)
	}

	// Stream stdout and stderr concurrently to the Logger.
	var wg sync.WaitGroup
	stream := func(r *bufio.Scanner) {
		defer wg.Done()
		for r.Scan() {
			e.Log(r.Text())
		}
	}

	wg.Add(2)
	go stream(bufio.NewScanner(stdout))
	go stream(bufio.NewScanner(stderr))
	wg.Wait()

	if err := cmd.Wait(); err != nil {
		if e.IsKilled() {
			return fmt.Errorf("task was stopped by user")
		}
		return fmt.Errorf("container exited with error: %w", err)
	}

	return nil
}

// buildPodmanArgs assembles the full `podman run` argument list.
func (e *Executor) buildPodmanArgs(image string) []string {
	repoPath := e.Repository.GetFullPath(e.Template.ID)

	args := []string{
		"run",
		"--rm",                                       // remove container after run
		"--network", e.cfg.Network,                   // network mode
		"--user", e.cfg.RunAsUser,                    // non-root enforcement
		"-v", repoPath + ":/runner/project:ro,z",     // repository (read-only)
		"-v", e.inventoryFilePath + ":/runner/inventory:ro,z", // inventory (read-only)
		"-w", "/runner/project",                      // working directory inside container
	}

	// Inject credentials and config as env vars — never written to disk.
	for _, env := range e.buildEnvVars() {
		args = append(args, "-e", env)
	}

	args = append(args, image)

	// The entrypoint is defined in the EE image itself (e.g. ansible-runner).
	// Additional arguments can be added here in Phase 2 when the App type is known.

	return args
}

// buildEnvVars returns NAME=value pairs to pass into the container.
// Secrets are included here and masked in the Logger by Semaphore's existing
// masking infrastructure (task_logger).
func (e *Executor) buildEnvVars() []string {
	var env []string

	// Pass the Semaphore JWT so the container can call back to the API.
	if e.JWT != "" {
		env = append(env, "SEMAPHORE_JWT="+e.JWT)
	}

	// Pass environment variables defined in the Semaphore Environment.
	if e.Environment.ENV != nil {
		envVars := parseEnvJSON(*e.Environment.ENV)
		env = append(env, envVars...)
	}

	// Pass secrets defined in the Environment (type: env).
	for _, secret := range e.Environment.Secrets {
		if secret.Type == db.EnvironmentSecretEnv {
			env = append(env, secret.Name+"="+secret.Secret)
		}
	}

	return env
}

// updateRepository clones or pulls the repository into the shared working copy.
func (e *Executor) updateRepository() error {
	unlock := e.RepoLock.Lock(e.Repository.GetFullPath(e.Template.ID))
	defer unlock()

	repo := db_lib.GitRepository{
		Logger:     e.Logger,
		TemplateID: e.Template.ID,
		Repository: e.Repository,
		Client:     db_lib.CreateDefaultGitClient(e.KeyInstaller),
	}

	err := repo.ValidateRepo()
	if err != nil {
		return repo.Clone()
	}

	if repo.CanBePulled() {
		if err := repo.Pull(); err == nil {
			return nil
		}
	}

	return repo.Clone()
}
