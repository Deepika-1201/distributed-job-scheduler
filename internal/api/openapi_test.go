package api

import (
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The OpenAPI document is the published contract; every route must appear in it and vice versa.
func TestOpenAPIMatchesRoutes(t *testing.T) {
	raw, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse openapi.yaml: %v", err)
	}
	var documented []string
	for path, ops := range doc.Paths {
		for method := range ops {
			if slices.Contains([]string{"get", "post", "put", "patch", "delete"}, method) {
				documented = append(documented, strings.ToUpper(method)+" "+path)
			}
		}
	}
	registered := New(nil, slog.New(slog.DiscardHandler), Config{TenantRateLimit: 1}).Routes()
	slices.Sort(documented)
	slices.Sort(registered)
	for _, r := range registered {
		if !slices.Contains(documented, r) {
			t.Errorf("route %q is not documented in api/openapi.yaml", r)
		}
	}
	for _, d := range documented {
		if !slices.Contains(registered, d) {
			t.Errorf("documented operation %q is not implemented", d)
		}
	}
}
