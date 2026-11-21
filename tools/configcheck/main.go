// Command configcheck validates every service's default TOML configuration.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

func main() {
	files, err := filepath.Glob("configs/*/etc-default.toml")
	if err != nil {
		panic(err)
	}
	if len(files) != 11 {
		fmt.Fprintf(os.Stderr, "expected 11 service configs, got %d\n", len(files))
		os.Exit(1)
	}
	for _, file := range files {
		var config map[string]any
		if _, err = toml.DecodeFile(file, &config); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", file, err)
			os.Exit(1)
		}
		zim, ok := config["zim"].(map[string]any)
		if !ok {
			fmt.Fprintf(os.Stderr, "%s: missing [zim]\n", file)
			os.Exit(1)
		}
		health, ok := zim["health"].(map[string]any)
		if !ok || health["addr"] == nil || health["addr"] == "" {
			fmt.Fprintf(os.Stderr, "%s: missing zim.health.addr\n", file)
			os.Exit(1)
		}
	}
	fmt.Printf("config check ok: %d services\n", len(files))
}
