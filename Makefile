.PHONY: help test test-integration build proto-gen proto-gen-check

# ── default ──────────────────────────────────────────────────────────────────
help:
	@echo "Available targets:"
	@echo "  make test              run unit tests"
	@echo "  make test-integration  run integration tests (requires embedded Postgres)"
	@echo "  make build             compile the server binary"
	@echo "  make proto-gen         regenerate proto/user_event.pb.go from .proto"
	@echo "                         WARNING: read the proto-gen target comments first"
	@echo "  make proto-gen-check   check that protoc and plugins are installed"

# ── tests ─────────────────────────────────────────────────────────────────────
test:
	GOTOOLCHAIN=auto go test ./...

test-integration:
	GOTOOLCHAIN=auto go test -tags=integration -v ./test/integration/... -timeout 180s

# ── build ─────────────────────────────────────────────────────────────────────
build:
	GOTOOLCHAIN=auto go build -o bin/server ./cmd/server

# ── proto ─────────────────────────────────────────────────────────────────────
#
# !! WARNING !!
# proto/user_event.pb.go is hand-maintained, NOT a clean protoc output.
# Running this target will OVERWRITE the file with protoc output and REMOVE:
#   - the DO NOT REGENERATE header
#   - the EventId field (field 7, json_name "event_id")
#
# After running proto-gen you MUST manually:
#   1. Re-add the DO NOT REGENERATE header block at the top of the file.
#   2. Re-add EventId to the UserEvent struct:
#        EventId string `json:"event_id"`
#   3. Verify `go build ./...` and `make test-integration` still pass.
#
# Protoc setup (one-time):
#   brew install protobuf                        # or: apt install protobuf-compiler
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   export PATH="$PATH:$(go env GOPATH)/bin"
#
proto-gen-check:
	@which protoc        > /dev/null 2>&1 || (echo "ERROR: protoc not found. Install protobuf-compiler." && exit 1)
	@which protoc-gen-go > /dev/null 2>&1 || (echo "ERROR: protoc-gen-go not found. Run: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	@echo "protoc and protoc-gen-go found."

proto-gen: proto-gen-check
	@echo ""
	@echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
	@echo "!! WARNING: proto/user_event.pb.go is hand-maintained.       !!"
	@echo "!! After this runs you MUST re-add EventId (field 7) and     !!"
	@echo "!! the DO NOT REGENERATE header. See Makefile comments.      !!"
	@echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
	@echo ""
	@echo "Proceeding in 5 seconds... (Ctrl-C to abort)"
	@sleep 5
	protoc \
		--proto_path=proto \
		--go_out=proto \
		--go_opt=paths=source_relative \
		proto/user_event.proto
	@echo ""
	@echo "Done. Now manually:"
	@echo "  1. Re-add the DO NOT REGENERATE header to proto/user_event.pb.go"
	@echo "  2. Re-add: EventId string \`json:\"event_id\"\` to the UserEvent struct"
	@echo "  3. Run: make test-integration"
