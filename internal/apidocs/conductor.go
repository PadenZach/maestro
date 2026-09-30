// Package apidocs exposes the implemented HTTP slice from its pinned contract.
package apidocs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/danielgtaylor/huma/v2"
)

// Conductor holds the official HTTP snapshot used to document enabled routes.
type Conductor struct {
	paths   map[string]any
	schemas map[string]any
}

// New loads a pinned official HTTP snapshot.
func New(snapshot []byte) (*Conductor, error) {
	root, err := decode(snapshot)
	if err != nil {
		return nil, fmt.Errorf("decode Conductor snapshot: %w", err)
	}
	paths, ok := root["paths"].(map[string]any)
	if !ok {
		return nil, errors.New("Conductor snapshot has no paths object")
	}
	components, _ := root["components"].(map[string]any)
	schemas, ok := components["schemas"].(map[string]any)
	if !ok {
		return nil, errors.New("Conductor snapshot has no component schemas object")
	}
	return &Conductor{paths: paths, schemas: schemas}, nil
}

// AddOperation documents one enabled route from the official HTTP snapshot.
func (c *Conductor) AddOperation(doc *huma.OpenAPI, method, sourcePath, servedPath string) (*huma.Operation, error) {
	if doc == nil {
		return nil, errors.New("nil OpenAPI document")
	}
	method = strings.ToUpper(method)
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE":
	default:
		return nil, fmt.Errorf("unsupported HTTP method %q", method)
	}
	item, _ := c.paths[sourcePath].(map[string]any)
	source, ok := item[strings.ToLower(method)].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("Conductor snapshot has no %s %s operation", method, sourcePath)
	}
	renames, err := parameterNames(sourcePath, servedPath)
	if err != nil {
		return nil, err
	}
	raw := clone(source).(map[string]any)
	id, ok := raw["operationId"].(string)
	if !ok || id == "" {
		return nil, fmt.Errorf("Conductor operation %s %s has no operationId", method, sourcePath)
	}
	for pathName, path := range doc.Paths {
		for existingMethod, existing := range operations(path) {
			if existing != nil && ((existingMethod == method && pathName == servedPath) || existing.OperationID == id) {
				return nil, fmt.Errorf("duplicate documented operation %s %s (%s)", method, servedPath, id)
			}
		}
	}
	parameters, _ := raw["parameters"].([]any)
	seen := map[string]bool{}
	for _, value := range parameters {
		parameter, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("Conductor operation has a malformed parameter")
		}
		if parameter["in"] != "path" {
			continue
		}
		name, _ := parameter["name"].(string)
		servedName, ok := renames[name]
		if !ok || seen[name] {
			return nil, fmt.Errorf("Conductor operation has unexpected path parameter %q", name)
		}
		seen[name] = true
		parameter["name"] = servedName
	}
	if len(seen) != len(renames) {
		return nil, errors.New("Conductor operation is missing a path parameter")
	}
	needed := map[string]map[string]any{}
	if err := c.references(raw, needed); err != nil {
		return nil, err
	}
	var registry huma.Registry
	if doc.Components != nil {
		registry = doc.Components.Schemas
	}
	if registry == nil {
		registry = huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)
	}
	for name, schema := range needed {
		if existing, exists := registry.Map()[name]; exists {
			b, err := json.Marshal(existing)
			if err != nil {
				return nil, fmt.Errorf("marshal existing schema %s: %w", name, err)
			}
			other, err := decode(b)
			if err != nil || !reflect.DeepEqual(other, schema) {
				return nil, fmt.Errorf("documented schema %s conflicts with the Conductor snapshot", name)
			}
		}
	}
	// Commit only after every reference and collision has been checked. Raw
	// extensions preserve OpenAPI 3.1 type unions and every pinned schema keyword.
	if doc.Components == nil {
		doc.Components = &huma.Components{}
	}
	doc.Components.Schemas = registry
	for name, schema := range needed {
		if _, exists := registry.Map()[name]; !exists {
			registry.Map()[name] = &huma.Schema{Extensions: clone(schema).(map[string]any)}
		}
	}
	op := &huma.Operation{Method: method, Path: servedPath, OperationID: id, Extensions: raw}
	doc.AddOperation(op)
	return op, nil
}

func decode(b []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var root map[string]any
	if err := d.Decode(&root); err != nil {
		return nil, err
	}
	if root == nil {
		return nil, errors.New("expected JSON object")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON")
	}
	return root, nil
}

func clone(value any) any {
	switch v := value.(type) {
	case map[string]any:
		copy := make(map[string]any, len(v))
		for key, child := range v {
			copy[key] = clone(child)
		}
		return copy
	case []any:
		copy := make([]any, len(v))
		for i, child := range v {
			copy[i] = clone(child)
		}
		return copy
	default:
		return value
	}
}

func parameterNames(sourcePath, servedPath string) (map[string]string, error) {
	source, served := strings.Split(sourcePath, "/"), strings.Split(servedPath, "/")
	if !strings.HasPrefix(servedPath, "/") || len(source) != len(served) {
		return nil, errors.New("served path does not match the Conductor path")
	}
	renames, used := map[string]string{}, map[string]bool{}
	for i, segment := range source {
		if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
			if !strings.HasPrefix(served[i], "{") || !strings.HasSuffix(served[i], "}") {
				return nil, errors.New("served path does not match the Conductor parameters")
			}
			name, replacement := segment[1:len(segment)-1], served[i][1:len(served[i])-1]
			if name == "" || replacement == "" || used[replacement] || strings.ContainsAny(replacement, "{}") {
				return nil, errors.New("served path has invalid or duplicate parameters")
			}
			renames[name], used[replacement] = replacement, true
		} else if segment != served[i] {
			return nil, errors.New("served path differs from the Conductor path")
		}
	}
	return renames, nil
}

func (c *Conductor) references(value any, needed map[string]map[string]any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if key == "$ref" {
				ref, ok := child.(string)
				const prefix = "#/components/schemas/"
				if !ok || !strings.HasPrefix(ref, prefix) {
					return fmt.Errorf("unsupported Conductor reference %v", child)
				}
				name := strings.TrimPrefix(ref, prefix)
				if _, exists := needed[name]; exists {
					continue
				}
				schema, ok := c.schemas[name].(map[string]any)
				if !ok {
					return fmt.Errorf("unresolved Conductor schema reference %q", ref)
				}
				needed[name] = schema
				if err := c.references(schema, needed); err != nil {
					return err
				}
			} else if err := c.references(child, needed); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := c.references(child, needed); err != nil {
				return err
			}
		}
	}
	return nil
}

func operations(item *huma.PathItem) map[string]*huma.Operation {
	if item == nil {
		return nil
	}
	return map[string]*huma.Operation{"GET": item.Get, "POST": item.Post, "PUT": item.Put, "PATCH": item.Patch, "DELETE": item.Delete, "HEAD": item.Head, "OPTIONS": item.Options, "TRACE": item.Trace}
}
