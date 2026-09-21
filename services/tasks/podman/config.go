// Package podman provides a UniFlow ExecutorProvider that runs each task inside
// an ephemeral Podman container. The image is pulled from any OCI-compatible
// registry; credentials, playbooks and inventory are injected at runtime so
// nothing sensitive is baked into the image.
//
// This is a UniFlow-specific addition to the open-source Semaphore codebase.
// It lives in its own package so upstream merges never touch it.
package podman

// Config holds the Podman-specific runner configuration.
// It maps to the runner config block:
//
//	runner:
//	  executor:
//	    type: podman
//	    podman:
//	      socket: unix:///run/user/1000/podman/podman.sock
//	      run_as_user: "1000"
//	      network: host
//	      pull_timeout: 300
type Config struct {
	// Socket is the Podman socket path (supports rootless sockets).
	// Defaults to the system socket /run/podman/podman.sock when empty.
	Socket string `json:"socket" yaml:"socket"`

	// RunAsUser sets the UID inside the container (non-root enforcement).
	// Defaults to "1000" when empty.
	RunAsUser string `json:"run_as_user" yaml:"run_as_user"`

	// Network sets the container network mode (host, bridge, none).
	// Defaults to "host" so Ansible can reach managed hosts without extra config.
	Network string `json:"network" yaml:"network"`

	// PullTimeoutSeconds is the maximum time in seconds to wait for an image pull.
	// Defaults to 300 (5 minutes).
	PullTimeoutSeconds int `json:"pull_timeout_seconds" yaml:"pull_timeout_seconds"`
}

// defaults fills in zero-value fields with sensible defaults.
func (c *Config) defaults() {
	if c.Socket == "" {
		c.Socket = "unix:///run/podman/podman.sock"
	}
	if c.RunAsUser == "" {
		c.RunAsUser = "1000"
	}
	if c.Network == "" {
		c.Network = "host"
	}
	if c.PullTimeoutSeconds == 0 {
		c.PullTimeoutSeconds = 300
	}
}
