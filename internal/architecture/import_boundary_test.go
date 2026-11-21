package architecture_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAPILayerDoesNotImportDAO(t *testing.T) {
	root := filepath.Join("..", "..", "cmd")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		if !strings.Contains(filepath.ToSlash(path), "/app/api/") {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		for _, spec := range file.Imports {
			if strings.Contains(spec.Path.Value, "/internal/dao/") {
				t.Errorf("%s imports DAO directly", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestZimfwDoesNotImportInternal(t *testing.T) {
	root := filepath.Join("..", "..", "zimfw")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		for _, spec := range file.Imports {
			if strings.Contains(spec.Path.Value, "github.com/zciyuan/ZIM/internal") {
				t.Errorf("%s imports internal package", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
