package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/runutil"
	"gopkg.in/alecthomas/kingpin.v2"
)

const corruptionRepro = "corruption_reproduce"

func registerCorruptionRepro(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, `
Command that infinitely compacts the same block in place. Each compact produces really the same block just with different ULID.
However it runs proper compaction, excactly the same as Thanos and Prometheus, so it touches all the series and build index from scratch.

This allows to detect potential corruption on download / storage / upload or compaction itself in order to reproduce issues like: https://github.com/thanos-io/thanos/issues/1300

Example: 

go run cmd/thanostest/*.go corruption_reproduce --data-dir=tmp --starting-block="<ULID of some pre-exiting block of your choice from the bucket>" --objstore.config="
type: S3
config:
  bucket: <some tmp bucket (don't use production one!)>
  endpoint: <endpoint>
  access_key: <access key>
  secret_key: <secret>
"`)

	httpMetricsBindAddr := regHTTPAddrFlag(cmd)

	bktConfig := regCommonObjStoreFlags(cmd, "", true)

	dataDir := cmd.Flag("data-dir", "Local directory to use").Required().String()

	startBlockULID := cmd.Flag("starting-block", "ULID of the pre-existing block in the bucket that should be used as initial block").Required().String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry) error {
		logger = log.With(logger, "component", corruptionRepro)

		level.Debug(logger).Log("msg", "setting up metric http listen-group")

		// TODO: There is listener so you can add metrics as well (:
		if err := metricHTTPListenGroup(g, logger, reg, *httpMetricsBindAddr); err != nil {
			return err
		}

		configYaml, err := bktConfig.Content()
		if err != nil {
			return err
		}

		if len(configYaml) == 0 {
			return errors.New("No supported bucket was configured")
		}

		bkt, err := client.NewBucket(
			logger,
			configYaml,
			reg,
			corruptionRepro,
		)
		if err != nil {
			return err
		}

		// TODO: Add some metrics if we wish to run it for longer like compactions, failures etc.

		current, err := ulid.Parse(*startBlockULID)
		if err != nil {
			return errors.Wrap(err, "not an ULID")
		}

		ctx, cancel := context.WithCancel(context.Background())
		comp, err := tsdb.NewLeveledCompactor(ctx, reg, logger, []int64{1}, nil)
		if err != nil {
			cancel()
			return errors.Wrap(err, "create compactor")
		}

		var (
			firstBlock = current
			num        = 0
			workDir    = filepath.Join(*dataDir, corruptionRepro)
		)

		level.Info(logger).Log("msg", "cleaning up workdir", "workDir", workDir)
		if err := os.RemoveAll(workDir); err != nil {
			return err
		}
		if err := os.MkdirAll(workDir, os.ModePerm); err != nil {
			return err
		}

		// Schedule run group that will be ran in as part of this command.
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				start := time.Now()

				// Non stop.

				level.Info(logger).Log("msg", "running test compaction in place for block", "id", current)

				bDir := filepath.Join(workDir, current.String())
				level.Info(logger).Log("msg", "downloading block", "id", current, "targetDir", bDir)
				if err := block.Download(ctx, logger, bkt, current, bDir); err != nil {
					return errors.Wrap(err, "download")
				}

				b, err := tsdb.OpenBlock(logger, bDir, nil)
				if err != nil {
					return errors.Wrap(err, "open block")
				}

				m, err := metadata.Read(filepath.Join(workDir, current.String()))
				if err != nil {
					return errors.Wrap(err, "get meta")
				}

				level.Info(logger).Log("msg", "compacting in place", "id", current)
				newID, err := comp.Write(workDir, b, b.MinTime(), b.MaxTime(), &m.BlockMeta)
				if err != nil {
					return errors.Wrap(err, "compact, is block corrupted?")
				}

				level.Info(logger).Log("msg", "compacted into new block", "id", current, "new", newID)

				newDir := filepath.Join(workDir, newID.String())
				newMeta, err := metadata.InjectThanos(logger, newDir, metadata.Thanos{
					// Just to make sure we know that it's a test.
					Labels:     map[string]string{"yolo": "thanostest-corruption-reproduce"},
					Downsample: m.Thanos.Downsample,
					Source:     "thanostest-corruption-reproduce",
				}, nil)
				if err != nil {
					return errors.Wrapf(err, "failed to finalize the block %s", newDir)
				}

				if err = os.Remove(filepath.Join(newDir, "tombstones")); err != nil {
					return errors.Wrap(err, "remove tombstones")
				}

				// Ensure the output block is valid.
				if err := block.VerifyIndex(logger, filepath.Join(newDir, block.IndexFilename), newMeta.MinTime, newMeta.MaxTime); err != nil {
					return errors.Wrap(err, "index verify")
				}

				level.Info(logger).Log("msg", "uploading block", "new", newID)
				if err := block.Upload(ctx, logger, bkt, newDir); err != nil {
					return errors.Wrapf(err, "upload of %s failed", newID)
				}
				level.Info(logger).Log("msg", "uploaded block", "new", newID)

				// Delete only new test blocks, don't touch the initial, source one.
				if current.Compare(firstBlock) != 0 {
					level.Info(logger).Log("msg", "deleting old block", "id", current)
					if err := block.Delete(ctx, logger, bkt, current); err != nil {
						return errors.Wrapf(err, "delete old block from bucket")
					}
					level.Info(logger).Log("msg", "deleted old block", "id", current)
				}

				level.Info(logger).Log("msg", "deleting locally old and new blocks", "id", current, "new", newID)
				if err := os.RemoveAll(newDir); err != nil {
					return errors.Wrap(err, "delete new block")
				}
				if err := os.RemoveAll(filepath.Join(workDir, current.String())); err != nil {
					return errors.Wrap(err, "delete old block")
				}

				num++
				level.Info(logger).Log("msg", "compaction in place done", "elapsed", time.Since(start), "operations", num)

				current = newID
			}
		}, func(error) {
			cancel()
		})

		level.Info(logger).Log("msg", "starting corruption reproduce")
		return nil
	}
}
