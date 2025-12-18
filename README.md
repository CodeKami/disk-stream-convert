# disk-stream-convert

A Go project for streaming disk image conversion that provides both a command-line tool and an HTTP service.

- Primary use: bidirectional conversion between `raw` and `vmdk (streamOptimized)`. Supports local file-to-file conversion, streaming import from a URL to local storage, and online conversion via HTTP upload/download.
- Supported formats: `raw`, `vmdk` (streamOptimized). The repository includes some `qcow2` type definitions, but the current HTTP and CLI conversion only implement `raw` and `vmdk`.

## Build

- Requirement: `Go 1.21+`
- Build commands:
  - HTTP server: `go build -o ./bin/dsc-server ./cmd/server`
  - CLI converter: `go build -o ./bin/dsc-convert ./cmd/convert`

## Command-line Usage (converter)

Binary: `dsc-convert`

Flags:
- `-src` source path or URL (`http://`, `https://` supported)
- `-dst` destination local file path
- `-src-fmt` source format: `raw` or `vmdk`
- `-dst-fmt` destination format: `raw` or `vmdk` (default `raw`)
- `-prealloc` whether to preallocate capacity for `raw` destination (default false)

Examples:
- Local `raw` → local `vmdk`:
  ```
  ./bin/dsc-convert -src /path/disk.raw -dst /path/disk.vmdk -src-fmt raw -dst-fmt vmdk
  ```
- Local `vmdk` → local `raw`:
  ```
  ./bin/dsc-convert -src /path/disk.vmdk -dst /path/disk.raw -src-fmt vmdk -dst-fmt raw
  ```
- Convert directly from URL to local (URL → local):
  ```
  ./bin/dsc-convert -src https://example.com/disk.raw -dst /path/disk.vmdk -src-fmt raw -dst-fmt vmdk
  ```
- Preallocate `raw` destination:
  ```
  ./bin/dsc-convert -src /path/disk.vmdk -dst /path/disk.raw -src-fmt vmdk -dst-fmt raw -prealloc
  ```

## HTTP Service

Binary: `dsc-server`

Start:
- Specify output directory (server writes converted local files to this directory):
  ```
  ./bin/dsc-server -outdir /tmp/disk-streams
  ```
- Listen address: `:8080`
- Routes: `/upload`, `/import`, `/export`

### Upload and Convert (/upload)

- Method: `POST`
- Description: Client uploads data; server converts it to the specified format and writes to the local output directory.
- Supported content types:
  - `multipart/form-data`, field name: `file`
  - `application/octet-stream` (request body is the data)
- Query parameters:
  - `src` source format: `raw`, `vmdk`
  - `dst` destination format: `raw`, `vmdk`
  - `prealloc` whether to preallocate (only effective when `dst=raw`, `true`/`false`)
  - `name` output filename (optional, default `upload.img`)
- Response (JSON):
  - `output` output file path
  - `writtenBytes` actual written bytes
  - `capacityBytes` target image capacity in bytes
  - `elapsedSeconds` conversion time in seconds
- Examples:
  - Upload `raw` as octet-stream and convert to `vmdk`:
    ```
    curl -X POST "http://localhost:8080/upload?src=raw&dst=vmdk&name=raw2vmdk.vmdk" \
      -H "Content-Type: application/octet-stream" \
      --data-binary @/path/disk.raw
    ```
  - Upload `vmdk` via multipart and convert to `raw` with preallocation:
    ```
    curl -X POST "http://localhost:8080/upload?src=vmdk&dst=raw&prealloc=true&name=disk.raw" \
      -F "file=@/path/disk.vmdk"
    ```

### Import from URL and Convert (/import)

- Method: `GET` or `POST`
- Description: Server streams the source image from the specified URL and converts it to the local output directory.
- GET query parameters:
  - `url` source file URL
  - `src` source format: `raw`, `vmdk`
  - `dst` destination format: `raw`, `vmdk`
  - `prealloc` whether to preallocate (only effective when `dst=raw`)
- POST request body (`application/json`):
  ```json
  { "url": "https://example.com/disk.raw", "src": "raw", "dst": "vmdk", "prealloc": false }
  ```
- Response (JSON): same as `/upload`
- Examples (GET):
  ```
  curl "http://localhost:8080/import?url=https://example.com/disk.vmdk&src=vmdk&dst=raw&prealloc=true"
  ```
- Examples (POST):
  ```
  curl -X POST "http://localhost:8080/import" \
    -H "Content-Type: application/json" \
    -d '{"url":"https://example.com/disk.raw","src":"raw","dst":"vmdk"}'
  ```

### Online Convert and Download (/export)

- Method: `GET`
- Description: Read the source image from a local file, convert it to the target format online, and return it as the response body.
- Query parameters:
  - `path` local source file path
  - `src` source format: `raw`, `vmdk`
  - `dst` destination format: `raw`, `vmdk`
- Response:
  - `Content-Type: application/octet-stream`
  - `Content-Disposition: attachment; filename="<generated filename>"`
    When `dst=vmdk`, the extension is changed to `.vmdk`
- Example:
  ```
  curl -OJ "http://localhost:8080/export?src=raw&dst=vmdk&path=/tmp/disk-streams/disk.raw"
  ```

## How It Works

- Reader (`pkg/diskfmt/... Reader`) parses the data stream according to the format and returns data blocks with logical offsets; for example, the `vmdk` Reader follows the `streamOptimized` structure and outputs grain-by-grain decompressed data.
- Writer (`pkg/diskfmt/... Writer`) writes data blocks sequentially and fills zero bytes for holes in logical offsets; the `raw` Writer can preallocate capacity, while the `vmdk` Writer generates header, descriptor, Grain Table/Directory, and footer markers following the `streamOptimized` spec.
- Core converter (`pkg/converter/converter.go`) reads blocks in a loop, handles offset gaps (zero filling), writes to destination, and ensures the final capacity matches the source image's declared capacity.

## Notes

- Currently only `raw` ↔ `vmdk` conversion is implemented. To add more formats, implement corresponding Reader/Writer under `pkg/diskfmt` and integrate them in the server/CLI.
- The server's local output directory is specified via `-outdir`; ensure write permissions and sufficient disk space.
- `/export` performs online conversion and download. If an error occurs after streaming starts, the HTTP status cannot be changed; check server logs instead.
