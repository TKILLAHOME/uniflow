package podman

import (
	"sync"

	"github.com/semaphoreui/semaphore/db"
	"github.com/semaphoreui/semaphore/db_lib"
	"github.com/semaphoreui/semaphore/services/tasks"
)

// Provider implements tasks.ExecutorProvider for Podman-backed execution.
// One Provider is created at runner startup and shared across all tasks.
// It holds the Podman config and a shared repo lock map — the same pattern
// LocalExecutorProvider uses.
type Provider struct {
	cfg          Config
	keyInstaller db_lib.AccessKeyInstaller

	// repoLocks serialises git operations per repository path so parallel
	// tasks sharing the same working copy don't race on git pull/checkout.
	repoLocksMu sync.Mutex
	repoLocks   map[string]*tasks.KeyLock
}

// NewProvider constructs a Provider and validates the Podman socket is reachable.
// Called once at runner startup by executor_factory.go.
func NewProvider(cfg Config, keyInstaller db_lib.AccessKeyInstaller) (tasks.ExecutorProvider, error) {
	cfg.defaults()

	p := &Provider{
		cfg:          cfg,
		keyInstaller: keyInstaller,
		repoLocks:    make(map[string]*tasks.KeyLock),
	}

	return p, nil
}

// NewExecutor builds a per-task PodmanExecutor. Called by the job pool for
// every queued task that is dispatched to this provider.
func (p *Provider) NewExecutor(
	task db.Task,
	template db.Template,
	inventory db.Inventory,
	repository db.Repository,
	environment db.Environment,
	jwt string,
) (tasks.Executor, error) {

	repoLock := p.getRepoLock(repository.GetFullPath(template.ID))

	return &Executor{
		Task:         task,
		Template:     template,
		Inventory:    inventory,
		Repository:   repository,
		Environment:  environment,
		JWT:          jwt,
		cfg:          p.cfg,
		KeyInstaller: p.keyInstaller,
		RepoLock:     repoLock,
	}, nil
}

// getRepoLock returns (creating if needed) the KeyLock for a repository path.
// The same lock is shared by all tasks that use the same working-copy directory.
func (p *Provider) getRepoLock(repoPath string) *tasks.KeyLock {
	p.repoLocksMu.Lock()
	defer p.repoLocksMu.Unlock()

	if lock, ok := p.repoLocks[repoPath]; ok {
		return lock
	}

	lock := &tasks.KeyLock{}
	p.repoLocks[repoPath] = lock
	return lock
}
