// The analytics batch projection lives in sink.go (Parse, Sink, BatchHandler,
// PostgresSink). This file only carries the package clause so a batcher with
// its own state can be added here later without breaking the build.
package analytics
