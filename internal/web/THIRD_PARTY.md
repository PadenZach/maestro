# Embedded UI dependencies

Alpine.js 3.17.4 is vendored from the official `alpinejs` npm release.

- Source: https://github.com/alpinejs/alpine/tree/v3.17.4
- Runtime: `static/alpine.min.js` (`dist/cdn.min.js` in the package)
- SHA-256: `232519394c6c8fdba6f362b1d9da16106db513cdbf899011f00daab4051df31c`
- License: MIT; [included notice](static/alpine-LICENSE.txt).

The runtime is embedded in the Go binary and served locally. No CDN or frontend
package installation is required to run maestro. Existing HTMX remains vendored
for server-rendered requests and live updates.
