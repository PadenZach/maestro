package console

import (
	"math"
	"net/http/httptest"
	"testing"
)

func TestAggregateSecondsExactPrecision(t *testing.T) {
	for _, test := range []struct {
		value string
		ms    int64
	}{{"0.001", 1}, {".5", 500}, {"1.234", 1234}, {"1.234000", 1234}, {"3600", 3600000}, {"9223372036854775.807", math.MaxInt64}} {
		got, err := aggregateSecondsMS(test.value)
		if err != nil || got != test.ms {
			t.Fatal(test.value, got, err)
		}
	}
	for _, value := range []string{"0", "0.0009", "1.2341", "-1", "1e3", "1.", ".", "NaN", "9223372036854775.808", "9999999999999999999999"} {
		if _, err := aggregateSecondsMS(value); err == nil {
			t.Fatal("invalid seconds accepted", value)
		}
	}
	sections := aggregateSections("workflows")
	fields, err := parseAggregateControls(httptest.NewRequest("GET", "/?timeBucketSeconds=9223372036854775.807&selectCount=true", nil), sections)
	if err != nil || fields["time_bucket_size_ms"] != int64(math.MaxInt64) {
		t.Fatal("int64 rounded", fields, err)
	}
	sections = aggregateSections("workflows")
	fields, err = parseAggregateControls(httptest.NewRequest("GET", "/?timeBucketSizeMs=1234&selectCount=true", nil), sections)
	if err != nil || fields["time_bucket_size_ms"] != int64(1234) || sections[0].Controls[len(sections[0].Controls)-1].Value != "1.234" {
		t.Fatal("legacy units lost", fields, err)
	}
}
