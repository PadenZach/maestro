package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"
)

func (s *handler) allowOrganization(w http.ResponseWriter, r *http.Request) bool {
	if r.PathValue("org") != s.cfg.OrgName {
		writeProblem(w, 404, "organization not found")
		return false
	}
	return true
}

func parseLimit(raw json.RawMessage, name string) (int, error) {
	if len(raw) == 0 || raw[0] < '0' || raw[0] > '9' {
		return 0, fmt.Errorf("%s must be a nonnegative integer", name)
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || value < 0 || int64(int(value)) != value {
		return 0, fmt.Errorf("%s must be a nonnegative representable int64", name)
	}
	return int(value), nil
}

func parseStepQuery(r *http.Request) (limit, offset *int, err error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, nil, errors.New("malformed query")
	}
	for name, values := range query {
		if name != "limit" && name != "offset" {
			return nil, nil, fmt.Errorf("unsupported query %q", name)
		}
		if len(values) != 1 {
			return nil, nil, fmt.Errorf("duplicate query %q", name)
		}
		n, e := parseLimit(json.RawMessage(values[0]), name)
		if e != nil {
			return nil, nil, e
		}
		if name == "limit" {
			limit = &n
		} else {
			offset = &n
		}
	}
	return
}

func parseUTF8Query(rawQuery string) (url.Values, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, errors.New("malformed query")
	}
	for name, values := range query {
		if !utf8.ValidString(name) {
			return nil, errors.New("malformed query")
		}
		for _, value := range values {
			if !utf8.ValidString(value) {
				return nil, errors.New("malformed query")
			}
		}
	}
	return query, nil
}

func validateApp(app string) error {
	if !utf8.ValidString(app) || utf8.RuneCountInString(app) < 3 || utf8.RuneCountInString(app) > 256 || !appName.MatchString(app) {
		return errors.New("invalid application name")
	}
	return nil
}
