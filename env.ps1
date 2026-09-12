# Activate the project-local toolchain (Go, protoc, make) — like a Python venv.
# Usage:  . .\env.ps1
$root = $PSScriptRoot
$env:GOROOT = "$root\.tools\go"
$env:GOPATH = "$root\.tools\gopath"
$env:GOBIN  = "$root\.tools\bin"
$env:GOFLAGS = "-mod=mod"
$env:CGO_ENABLED = "1"   # needed for `go test -race` (uses the gcc under .tools\winlibs)
$env:PATH = "$root\.tools\bin;$root\.tools\go\bin;$root\.tools\protoc\bin;$root\.tools\make\bin;$root\.tools\winlibs\mingw64\bin;$env:PATH"
Write-Host "toolchain active: $(go version) | protoc $(protoc --version) | $(make --version | Select-Object -First 1)"
