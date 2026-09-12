# Activate the project-local toolchain (Go, protoc, make) in Git Bash.
# Usage:  source ./env.sh
_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_win_root="$(cygpath -w "$_root")"
export GOROOT="$_win_root\\.tools\\go"
export GOPATH="$_win_root\\.tools\\gopath"
export GOBIN="$_win_root\\.tools\\bin"
export GOFLAGS="-mod=mod"
export CGO_ENABLED=1   # needed for `go test -race` (uses the gcc under .tools/winlibs)
export PATH="$_root/.tools/bin:$_root/.tools/go/bin:$_root/.tools/protoc/bin:$_root/.tools/make/bin:$_root/.tools/winlibs/mingw64/bin:$PATH"
echo "toolchain active: $(go version) | protoc $(protoc --version) | $(make --version | head -1)"
