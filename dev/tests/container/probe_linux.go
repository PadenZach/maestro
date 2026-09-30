// Command probe verifies the live container's security settings. It is mounted
// read-only for tests and is never included in the release image.
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
)

func main() {
	status, err := os.ReadFile("/proc/1/status")
	if err != nil {
		panic(err)
	}
	fields := map[string]string{}
	for line := range strings.SplitSeq(string(status), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			fields[key] = strings.Join(strings.Fields(value), " ")
		}
	}
	for key, want := range map[string]string{
		"Uid": "65532 65532 65532 65532", "Gid": "65532 65532 65532 65532",
		"CapEff": "0000000000000000", "CapPrm": "0000000000000000",
		"CapBnd": "0000000000000000", "NoNewPrivs": "1", "Seccomp": "2",
	} {
		if fields[key] != want {
			panic(fmt.Sprintf("%s = %q, want %q", key, fields[key], want))
		}
	}
	if err := os.WriteFile("/runtime-write-probe", nil, 0o600); !errors.Is(err, syscall.EROFS) {
		panic(fmt.Sprintf("root filesystem write returned %v, want EROFS", err))
	}
	fmt.Println("Verified non-root IDs, zero capabilities, no-new-privileges, seccomp filtering, read-only root")
}
