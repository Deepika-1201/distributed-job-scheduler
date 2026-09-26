package domain

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// The domain package must stay free of I/O and infrastructure dependencies (LLD §1).
func TestDomainImportsOnlyStandardLibrary(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			first, _, _ := strings.Cut(path, "/")
			if strings.Contains(first, ".") || first == "jobscheduler" {
				t.Errorf("%s imports %q; the domain package may import only the standard library", name, path)
			}
		}
	}
}
