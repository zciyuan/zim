//go:build mage

// Magefile is the single build entry point. It follows ZIM-03 §2.2.6: mage
// orchestrates, Docker builds images, and goreleaser publishes releases.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const (
	module     = "github.com/zciyuan/ZIM"
	versionPkg = module + "/internal/version"
	binDir     = "build/bin"
	zimfwDir   = "zimfw"
)

// Default is the default target.
var Default = Build

// Services is the single source of truth for the 11 IM binaries (ADR-0002).
var Services = []string{
	"zim-gate",
	"zim-node-msg",
	"zim-node-conv",
	"zim-mesh-auth",
	"zim-mesh-user",
	"zim-mesh-relation",
	"zim-mesh-group",
	"zim-transfer",
	"zim-push",
	"zim-api",
	"zim-crontask",
}

// Build compiles all 11 services. mage v1.17 does not support variadic
// targets, so a single service is built with BuildService.
func Build() error {
	return buildServices(Services)
}

// BuildService compiles one service. Usage: mage buildService zim-gate.
func BuildService(service string) error {
	return buildServices([]string{service})
}

// BuildAll compiles every service. It is an explicit alias for CI.
func BuildAll() error {
	return Build()
}

// buildServices is the shared implementation for Build and BuildService.
func buildServices(svcs []string) error {
	mg.Deps(ensureOutput)

	known := make(map[string]struct{}, len(Services))
	for _, service := range Services {
		known[service] = struct{}{}
	}
	for _, service := range svcs {
		if _, ok := known[service]; !ok {
			return fmt.Errorf("unknown service %q", service)
		}
		if _, err := os.Stat(filepath.Join("cmd", service)); err != nil {
			return fmt.Errorf("cmd/%s is missing: %w", service, err)
		}
	}

	ldflags := fmt.Sprintf(
		"-s -w -X %s.Version=%s -X %s.Commit=%s -X %s.BuildTime=%s",
		versionPkg, version(),
		versionPkg, commit(),
		versionPkg, time.Now().UTC().Format(time.RFC3339),
	)

	for _, service := range svcs {
		fmt.Printf("→ build %s (%s/%s)\n", service, runtime.GOOS, runtime.GOARCH)
		if err := sh.RunV("go", "build", "-trimpath", "-buildvcs=false",
			"-ldflags", ldflags,
			"-o", filepath.Join(binDir, executable(service)),
			"./cmd/"+service,
		); err != nil {
			return fmt.Errorf("build %s: %w", service, err)
		}
	}
	return nil
}

// Test runs unit tests with the race detector in both Go modules.
func Test() error {
	mg.Deps(TestZimfw)
	return sh.RunV("go", "test", "-race", "-count=1", "./...")
}

// TestZimfw runs the framework module tests with coverage.
func TestZimfw() error {
	return runIn(zimfwDir, "go", "test", "-race", "-count=1", "-cover", "./...")
}

// Cover generates a combined coverage report for the root and framework modules.
func Cover() error {
	mg.Deps(ensureOutput)
	if err := sh.RunV("go", "test", "-count=1", "-coverprofile", filepath.Join("_output", "coverage-root.out"), "./..."); err != nil {
		return err
	}
	if err := runIn(zimfwDir, "go", "test", "-count=1", "-coverprofile",
		filepath.Join("..", "_output", "coverage-zimfw.out"), "./..."); err != nil {
		return err
	}
	return sh.RunV("go", "tool", "cover", "-html", filepath.Join("_output", "coverage-root.out"),
		"-o", filepath.Join("_output", "coverage.html"))
}

// Lint runs golangci-lint in both modules.
func Lint() error {
	if err := sh.RunV("golangci-lint", "run", "--timeout", "5m", "./..."); err != nil {
		return err
	}
	return runIn(zimfwDir, "golangci-lint", "run", "--timeout", "5m", "./...")
}

// Generate regenerates protobuf code from the checked-in .proto files.
func Generate() error {
	if err := sh.RunV("protoc",
		"--go_out=zimfw/contract", "--go_opt=paths=source_relative",
		"-I", "zimfw/contract", "zimfw/contract/envelope.proto",
	); err != nil {
		return err
	}
	if err := sh.RunV("protoc",
		"--go_out=internal/protocol/auth", "--go_opt=paths=source_relative",
		"-I", "internal/protocol/auth", "internal/protocol/auth/login.proto",
	); err != nil {
		return err
	}
	if err := sh.RunV("protoc",
		"--go_out=internal/protocol/sync", "--go_opt=paths=source_relative",
		"-I", "internal/protocol/sync", "internal/protocol/sync/sync.proto",
	); err != nil {
		return err
	}
	if err := sh.RunV("protoc",
		"--go_out=internal/protocol/rtc", "--go_opt=paths=source_relative",
		"-I", "internal/protocol/rtc", "internal/protocol/rtc/rtc_client.proto",
	); err != nil {
		return err
	}
	return sh.RunV("protoc",
		"--go_out=internal/service/auth/pb", "--go_opt=paths=source_relative",
		"--go-grpc_out=internal/service/auth/pb", "--go-grpc_opt=paths=source_relative",
		"-I", "internal/service/auth/pb", "internal/service/auth/pb/auth.proto",
	)
}

// ReplayDLQ replays dead-letter records back to their original topic.
// Usage: mage replayDLQ zim.pipe.toRedis.DLQ 127.0.0.1:9092 false
func ReplayDLQ(dlq, brokers string, dryRun bool) error {
	args := []string{"run", "./tools/dlqreplay", "-dlq", dlq}
	if brokers != "" {
		args = append(args, "-brokers", brokers)
	}
	if dryRun {
		args = append(args, "-dry-run")
	}
	return sh.RunV("go", args...)
}

// ImageOCI builds a multi-arch OCI archive without registry push.
func ImageOCI() error {
	return sh.RunV("docker", "buildx", "build",
		"--platform", "linux/amd64,linux/arm64",
		"--build-arg", "VERSION="+version(),
		"--build-arg", "COMMIT="+commit(),
		"-f", "deploy/docker/Dockerfile",
		"-t", "zim-server:multi",
		"--output", "type=oci,dest=run/zim-multi.tar",
		".",
	)
}

// Image builds the multi-arch single image containing all 11 binaries.
func Image() error {
	return sh.RunV("docker", "buildx", "build",
		"--platform", "linux/amd64,linux/arm64",
		"--build-arg", "VERSION="+version(),
		"--build-arg", "COMMIT="+commit(),
		"-f", "deploy/docker/Dockerfile",
		"-t", "registry.local/zim/zim-server:"+version(),
		"--push", ".",
	)
}

// ConfigCheck validates all service default TOML files.
func ConfigCheck() error {
	return sh.RunV("go", "run", "./tools/configcheck")
}

// Clean removes build output.
func Clean() error {
	return sh.Rm("_output")
}

// ensureOutput creates the binary output directory.
func ensureOutput() error {
	return os.MkdirAll(binDir, 0o755)
}

// version returns the current git tag or a development marker.
func version() string {
	out, err := sh.Output("git", "describe", "--tags", "--always")
	if err != nil || strings.TrimSpace(out) == "" {
		return "dev"
	}
	return strings.TrimSpace(out)
}

// commit returns the current git revision or a development marker.
func commit() string {
	out, err := sh.Output("git", "rev-parse", "--short", "HEAD")
	if err != nil || strings.TrimSpace(out) == "" {
		return "unknown"
	}
	return strings.TrimSpace(out)
}

// executable appends the platform suffix on Windows.
func executable(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

// runIn runs a command with the working directory set to dir.
func runIn(dir, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s in %s: %w", name, dir, err)
	}
	return nil
}
