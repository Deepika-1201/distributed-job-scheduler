package api

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"jobscheduler/internal/domain"
)

const (
	maxSchemaBytes     = 64 << 10
	maxCachedSchemas   = 1000
	maxViolationLength = 1 << 10
	schemaURL          = "mem:///payload-schema.json"
)

// denyLoader refuses every external $ref: schemas come from tenants, and the library's
// default loader reads local files (ADR-019).
type denyLoader struct{}

func (denyLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external references are not allowed (%s); use #/$defs instead", url)
}

func compileSchema(raw []byte) (*jsonschema.Schema, error) {
	if len(raw) > maxSchemaBytes {
		return nil, fmt.Errorf("schema exceeds %d bytes", maxSchemaBytes)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("schema is not valid JSON: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.UseLoader(denyLoader{})
	c.DefaultDraft(jsonschema.Draft2020)
	if err := c.AddResource(schemaURL, doc); err != nil {
		return nil, err
	}
	return c.Compile(schemaURL)
}

type schemaKey struct {
	tenant  domain.TenantID
	name    string
	updated time.Time
}

// schemaCache holds compiled schemas per job type version; an update changes the key.
type schemaCache struct {
	mu      sync.Mutex
	entries map[schemaKey]*jsonschema.Schema
}

func (c *schemaCache) get(jt domain.JobType) (*jsonschema.Schema, error) {
	key := schemaKey{jt.TenantID, jt.Name, jt.UpdatedAt}
	c.mu.Lock()
	s, ok := c.entries[key]
	c.mu.Unlock()
	if ok {
		return s, nil
	}
	s, err := compileSchema(jt.PayloadSchema)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) >= maxCachedSchemas {
		c.entries = map[schemaKey]*jsonschema.Schema{}
	}
	c.entries[key] = s
	return s, nil
}

// validatePayload checks a payload against its job type's schema, if it has one.
func (s *Server) validatePayload(jt domain.JobType, payload []byte) error {
	if len(jt.PayloadSchema) == 0 {
		return nil
	}
	schema, err := s.schemas.get(jt)
	if err != nil {
		return fmt.Errorf("compile stored schema of job type %s: %w", jt.Name, err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fieldErrors{"payload": "must be valid JSON"}.err()
	}
	if err := schema.Validate(doc); err != nil {
		msg := strings.ReplaceAll(err.Error(), schemaURL, "payload_schema")
		if len(msg) > maxViolationLength {
			msg = msg[:maxViolationLength] + "…"
		}
		return fieldErrors{"payload": msg}.err()
	}
	return nil
}
