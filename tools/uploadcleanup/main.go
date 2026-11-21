// Command uploadcleanup removes local upload objects older than a retention age.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	dir := flag.String("dir", "run/upload", "upload root")
	age := flag.Duration("age", 30*24*time.Hour, "minimum object age")
	dry := flag.Bool("dry-run", true, "only print matches")
	flag.Parse()

	cutoff := time.Now().Add(-*age)
	removed := 0
	err := filepath.WalkDir(*dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, infoErr := entry.Info()
		if infoErr != nil || info.ModTime().After(cutoff) {
			return infoErr
		}
		if *dry {
			fmt.Println("would remove", path)
			return nil
		}
		if removeErr := os.Remove(path); removeErr != nil {
			return removeErr
		}
		removed++
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("removed=%d dry_run=%v\n", removed, *dry)
}
