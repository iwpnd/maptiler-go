package version

import "runtime/debug"

var (
	Version   = "edge"
	GitSHA    = "unknown"
	BuildTime = "unknown"
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "(devel)" {
		Version = info.Main.Version
	}
}
