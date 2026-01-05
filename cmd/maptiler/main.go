package main

import (
	"fmt"

	"github.com/iwpnd/maptiler-go/cmd/maptiler/version"
)

func main() {
	fmt.Printf("version: %s\n", version.Version)
	fmt.Printf("build time: %s\n", version.BuildTime)
	fmt.Printf("git sha: %s\n", version.GitSHA)
}
