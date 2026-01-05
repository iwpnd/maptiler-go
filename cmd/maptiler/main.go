package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/iwpnd/maptiler-go"
	"github.com/iwpnd/maptiler-go/cmd/maptiler/version"
)

func main() {
	app := &cli.Command{
		Name:  "maptilerctl",
		Usage: "CLI for MapTiler dataset ingestion (create/update/cancel)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "host",
				Usage:   "MapTiler service host (defaults to https://service.maptiler.com/v1)",
				Sources: cli.EnvVars("MAPTILER_HOST"),
			},
			&cli.StringFlag{
				Name:    "token",
				Usage:   "MapTiler API token (falls back to MAPTILER_TOKEN)",
				Sources: cli.EnvVars("MAPTILER_TOKEN"),
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "Request timeout (0 = no explicit timeout)",
				Value: 10 * time.Minute,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "version",
				Usage: "print cli version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					//nolint:errcheck
					fmt.Fprintf(os.Stdout, `
      Version:    %s
      Commit:     %s
      Build Time: %s
    `, version.Version, version.GitSHA, version.BuildTime)
					return nil
				},
			},
			{
				Name:  "get",
				Usage: "get an upload by id",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "Dataset ID to get",
						Required: true,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					c, cctx, cancel, err := newClientWithContext(ctx, cmd)
					if err != nil {
						return err
					}
					defer cancel()

					id := cmd.String("id")
					ir, err := c.Get(cctx, id)
					if err != nil {
						return err
					}
					fmt.Println(ir.String())
					return nil
				},
			},
			{
				Name:  "create",
				Usage: "Create a new dataset ingestion from a file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "file",
						Aliases:  []string{"f"},
						Usage:    "Path to the dataset file to ingest",
						Required: true,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					c, cctx, cancel, err := newClientWithContext(ctx, cmd)
					if err != nil {
						return err
					}
					defer cancel()

					fp := cmd.String("file")
					ir, err := c.Create(cctx, fp)
					if err != nil {
						return err
					}
					fmt.Println(ir.String())
					return nil
				},
			},
			{
				Name:  "update",
				Usage: "Update an existing dataset ingestion by dataset ID using a file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "Dataset ID to update",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "file",
						Aliases:  []string{"f"},
						Usage:    "Path to the dataset file to ingest",
						Required: true,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					c, cctx, cancel, err := newClientWithContext(ctx, cmd)
					if err != nil {
						return err
					}
					defer cancel()

					id := cmd.String("id")
					fp := cmd.String("file")
					ir, err := c.Update(cctx, id, fp)
					if err != nil {
						return err
					}
					fmt.Println(ir.String())
					return nil
				},
			},
			{
				Name:  "cancel",
				Usage: "Cancel an ingest by ingest ID",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "Ingest ID to cancel",
						Required: true,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					c, cctx, cancel, err := newClientWithContext(ctx, cmd)
					if err != nil {
						return err
					}
					defer cancel()

					id := cmd.String("id")

					// Requires an exported Cancel method on your client.
					ir, err := c.Cancel(cctx, id)
					if err != nil {
						return err
					}
					fmt.Println(ir.String())
					return nil
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

// newClientWithContext creates the maptiler client, and also returns a context
// that cancels on SIGINT/SIGTERM and optionally applies a timeout.
func newClientWithContext(parent context.Context, cmd *cli.Command) (*maptiler.Client, context.Context, context.CancelFunc, error) {
	host := cmd.String("host")
	token := cmd.String("token")

	c, err := maptiler.New(host, token)
	if err != nil {
		return nil, nil, nil, err
	}

	// Cancel on Ctrl+C / termination.
	sigCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	timeout := cmd.Duration("timeout")

	if timeout > 0 {
		tctx, cancel := context.WithTimeout(sigCtx, timeout)
		// Combine: when tctx is canceled, we also stop signal notifications.
		return c, tctx, func() { cancel(); stop() }, nil
	}

	// No explicit timeout.
	return c, sigCtx, stop, nil
}
