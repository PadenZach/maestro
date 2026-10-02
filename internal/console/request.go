package console

import (
	"errors"
	"net/url"
	"unicode/utf8"
)

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
