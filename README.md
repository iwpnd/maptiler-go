# maptiler-go

> [!WARNING]
> Work in progress. Behavior and flags may change at any time.

A small CLI for interacting with the [MapTiler Dataset Ingestion API](https://docs.maptiler.com/cloud/admin-api/tileset_ingest/).
It wraps the MapTiler ingestion workflow (create, update, upload, finalize, cancel) into simple commands you can use from the terminal or scripts.

The CLI is built on top of the `maptiler` Go client and handles multipart uploads, retries, and cancellation for you.

## Features

* Create new dataset ingestions from local files
* Update existing datasets with new data
* Cancel in-flight ingestions
* Fetch ingestion status by ID
* Token-based authentication via flags or environment variables
* Context-aware cancellation and configurable timeouts

## Installation

```bash
go install github.com/iwpnd/cmd/maptilerctl@latest
```

Or build from source:

```bash
git clone github.com/iwpnd/maptiler-go
cd maptiler-go
make build
```

## Configuration

The CLI requires a MapTiler API token that you can create in your [MapTiler accoutn](https://cloud.maptiler.com/account/credentials/).

You can provide it via:

* `--token` flag
* `MAPTILER_TOKEN` environment variable (recommended)

Optional configuration:

* `--host` or `MAPTILER_HOST` to override the default service host
* `--timeout` to control request and upload duration

## Usage

```bash
maptilerctl [global options] [command [command options]]
```

### Global options

```text
--host string       MapTiler service host (defaults to https://service.maptiler.com/v1) [$MAPTILER_HOST]
--token string      MapTiler API token (falls back to MAPTILER_TOKEN) [$MAPTILER_TOKEN]
--timeout duration  Request timeout (0 = no explicit timeout) (default: 10m)
```

## Commands

### create

Create a new dataset ingestion from a local file.

```bash
maptilerctl create --file ./tiles.mbtiles
```

### update

Update an existing dataset by dataset ID.

```bash
maptilerctl update --id <dataset-id> --file ./tiles.mbtiles
```

### cancel

Cancel an in-flight ingestion by ingest ID.

```bash
maptilerctl cancel --id <ingest-id>
```

### get

Fetch the current state of an ingestion by ID.

```bash
maptilerctl get --id <ingest-id>
```

### version

Print the CLI version.

```bash
maptilerctl version
```

## Output

All commands print the raw JSON response returned by the MapTiler API:

```json
{
  "id": "019b8ef3-dcec-7ac1-b25b-31afcf8e5b76",
  "document_id": "019b8ef4-0263-74b8-b98f-b06dd64a5dbc",
  "state": "completed",
  "filename": "zwickau.mbtiles",
  "size": 28262400,
  "progress": 100,
  "errors": null
}
```

This makes the CLI easy to compose with tools like `jq`.

## Signals & Cancellation

`maptilerctl` listens for `SIGINT` / `SIGTERM`.

Pressing **Ctrl+C** during an upload will:

1. Cancel the local process
2. Send a cancellation request to the MapTiler API
3. Exit with an error if cancellation fails

## License

MIT

