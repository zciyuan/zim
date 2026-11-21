// Package version exposes build metadata injected at link time.
package version

import (
	"fmt"
	"os"
)

var (
	// Version is the release version, injected with -ldflags.
	Version = "dev"
	// Commit is the source revision, injected with -ldflags.
	Commit = "unknown"
	// BuildTime is the UTC build timestamp, injected with -ldflags.
	BuildTime = "unknown"
)

// String returns the human readable build metadata.
func String() string {
	return fmt.Sprintf("version=%s commit=%s build_time=%s", Version, Commit, BuildTime)
}

// HandleFlag prints build metadata and exits when --version or -version is
// present. It is called before due starts its container.
func HandleFlag() {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-version" {
			fmt.Println(String())
			os.Exit(0)
		}
	}
}
