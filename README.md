## Keep3r

Keep3r is a lightweight object store that exposes a minimal HTTP API for uploading blobs into bucket/key namespaces.  File contents are persisted on disk, while metadata such as size, checksum, and timestamps are tracked in an embedded BoltDB database.

### Features
- HTTP server powered by `chi` with a ready-to-use `/api/upload` endpoint.
- On-disk blob storage organized by bucket and key path.
- SHA-256 ETag calculation and metadata persistence via BoltDB.
- Health probe at `/health` for readiness checks.

### Getting Started
#### Requirements
- Go 1.25 or newer.

#### Clone and Build
```bash
git clone https://github.com/<your-org>/keep3r.git
cd keep3r
make build
```
You can also run `make` (default target) or `make run` to start the server after building.

#### Run the Server
```bash
make run
```
By default the server listens on `:8088`, stores object metadata under `./meta/meta.db`, and writes uploaded blobs to `./data/blobs`.

#### Clean build file
```bash
make clean
```

### Uploading Objects
Use query parameters or headers to specify the destination bucket and key when uploading a file.
```bash
curl -X PUT "http://localhost:8088/api/upload?bucket=photos&key=2024/cat.jpg" \
  -H "Content-Type: image/jpeg" \
  --data-binary @cat.jpg
```
Successful uploads return JSON metadata along with the computed ETag header.

### Configuration
Server options can be adjusted in `cmd/keep3r/main.go` by editing the `httpapi.Options` struct:
- `HTTPAddr`: address to bind the HTTP server (default `:8088`).
- `MetaDBPath`: location of the BoltDB metadata file (default `./meta/meta.db`).
- `DataRoot`: root directory for blob storage (default `./data/blobs`).

### HTTP API
- `GET /health` &mdash; returns `Status OK`.
- `PUT /api/upload` &mdash; uploads a blob. Requires both `bucket` and `key` supplied either as query parameters or headers (`X-Bucket`, `X-Key`).

### Development
- Format code with `make fmt`.
- Run `make test` to execute the test suite (tests coming soon).
- Keep changes well-scoped and include relevant documentation updates.
- Add unit and integration tests for new features; untested code is unlikely to be merged.

### Checkpoints
- **Core API parity**: finalize upload and future download/delete handlers, backed by metadata tests.
- **Observability**: replace placeholder TODOs with structured logging and error handling.
- **Operational readiness**: ship a sample systemd unit, container image, and deployment docs.
- **Community onboarding**: publish contribution guidelines, code of conduct, and release process.

### Contributing
1. Fork the repository and create a feature branch.
2. Make your changes with clear commits and accompanying tests when applicable.
3. Ensure the project builds (`make`) before opening a pull request.
4. Describe your changes and link any related issues.

### License
The project maintainers have not committed a license file yet.  Please open an issue or pull request if you would like to help formalize the licensing.
