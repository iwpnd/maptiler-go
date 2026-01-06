# maptiler-go

A small CLI for interacting with the [MapTiler Dataset Ingestion API](https://docs.maptiler.com/cloud/admin-api/tileset_ingest/).
It wraps the MapTiler ingestion workflow (create, update, upload, finalize, cancel) into simple commands you can use from the terminal or scripts.

The CLI is built on top of the `maptiler` Go client and handles multipart uploads, retries, and cancellation for you.

## Features

* create new dataset ingestions from local files
* update existing datasets with new data
* cancel in-flight ingestions
* fetch ingestion status by ID
* token-based authentication via flags or environment variables
* context-aware cancellation and configurable timeouts

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

```bash
# create: Create a new dataset ingestion from a local file.
maptilerctl create --file ./tiles.mbtiles

# update: Update an existing dataset by dataset ID.
maptilerctl update --id <dataset-id> --file ./tiles.mbtiles

# get: Fetch the current state of an ingestion by ID.
maptilerctl get --id <ingest-id>

# cancel: Cancel an in-flight ingestion by ingest ID.
maptilerctl cancel --id <ingest-id>
```

## Signals & Cancellation

`maptilerctl` listens for `SIGINT` / `SIGTERM`.

Pressing **Ctrl+C** during an upload will:

1. Cancel the local process
2. Send a cancellation request to the MapTiler API
3. Exit with an error if cancellation fails

## License

MIT

