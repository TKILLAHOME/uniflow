package podman

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/semaphoreui/semaphore/db"
	"github.com/semaphoreui/semaphore/db_lib"
	"github.com/semaphoreui/semaphore/pkg/task_logger"
)

// imageExists checks whether the image is already present in the local
// Podman image store, using `podman image exists <image>`.
func imageExists(ctx context.Context, image string) (bool, error) {
	cmd := exec.CommandContext(ctx, "podman", "image", "exists", image)
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	// Exit code 1 means "not found" — that's not an error for us.
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("podman image exists: %w", err)
}

// createTaskTmpDir creates an isolated tmp directory for a single task
// under the project tmp path. Returns the created directory path.
func createTaskTmpDir(projectTmpDir string, taskID int) (string, error) {
	dir := filepath.Join(projectTmpDir, fmt.Sprintf("task_%d", taskID))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("create task tmp dir: %w", err)
	}
	return dir, nil
}

// cleanTaskTmpDir removes the task tmp directory. Logs but does not return
// errors so Cleanup never blocks the job from being marked complete.
func cleanTaskTmpDir(dir string, logger task_logger.Logger) {
	if err := os.RemoveAll(dir); err != nil {
		if logger != nil {
			logger.Log("[UniFlow/podman] Warning: failed to clean tmp dir: " + err.Error())
		}
	}
}

// writeInventoryFile writes the Semaphore inventory to a file inside tmpDir
// and returns its path. Supports static and file-based inventory types.
func writeInventoryFile(
	tmpDir string,
	inventory db.Inventory,
	keyInstaller db_lib.AccessKeyInstaller,
	logger task_logger.Logger,
) (string, error) {
	inventoryPath := filepath.Join(tmpDir, "inventory")

	switch inventory.Type {
	case db.InventoryStatic, db.InventoryStaticYaml:
		if err := os.WriteFile(inventoryPath, []byte(inventory.Inventory), 0600); err != nil {
			return "", fmt.Errorf("write static inventory: %w", err)
		}
	case db.InventoryFile:
		// File inventory — path is provided by the user, resolved inside the container
		// at /runner/project/<path>. We write a pointer file so the executor knows
		// where to find it.
		inventoryPath = inventory.Inventory
	default:
		// For dynamic/other inventory types, write an empty placeholder.
		// Full dynamic inventory support is planned for Phase 2.
		if logger != nil {
			logger.Log(fmt.Sprintf("[UniFlow/podman] Inventory type %q: using path as-is", inventory.Type))
		}
		inventoryPath = inventory.Inventory
	}

	return inventoryPath, nil
}

// getEnvVar retrieves a named variable from the Semaphore Environment JSON.
// Returns an empty string when not found or on parse errors.
func getEnvVar(env db.Environment, name string) string {
	if env.JSON == "" {
		return ""
	}
	vars := make(map[string]string)
	if err := json.Unmarshal([]byte(env.JSON), &vars); err != nil {
		return ""
	}
	return vars[name]
}

// parseEnvJSON decodes a JSON object of string→string pairs into NAME=value
// slice suitable for `podman run -e`.
func parseEnvJSON(raw string) []string {
	if raw == "" {
		return nil
	}
	vars := make(map[string]string)
	if err := json.Unmarshal([]byte(raw), &vars); err != nil {
		return nil
	}
	result := make([]string, 0, len(vars))
	for k, v := range vars {
		// Sanitize: skip entries with newlines to prevent injection.
		if strings.ContainsAny(k+v, "\n\r") {
			continue
		}
		result = append(result, k+"="+v)
	}
	return result
}
