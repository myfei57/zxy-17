// Command taskflow starts a TaskFlow cluster process with an embedded console.
package main

import (
	"flag"
	"fmt"
	"log"

	"taskflow/internal/cluster"
	"taskflow/internal/settings"
)

func main() {
	var (
		addr    = flag.String("addr", "", "HTTP listen address")
		data    = flag.String("data", "", "data directory")
		conc    = flag.Int("concurrency", 0, "concurrency quota")
		nsCount = flag.Int("namespaces", 0, "number of namespaces")
	)
	flag.Parse()
	cfg := settings.Default()
	if *addr != "" {
		cfg.HTTPAddr = *addr
	}
	if *data != "" {
		cfg.DataDir = *data
	}
	if *conc > 0 {
		cfg.Concurrency = *conc
	}
	if *nsCount > 0 {
		cfg.NamespaceCount = *nsCount
	}
	cl, err := cluster.Build(cfg)
	if err != nil {
		log.Fatalf("taskflow: build: %v", err)
	}
	defer func() {
		if err := cl.Close(); err != nil {
			log.Printf("taskflow: close: %v", err)
		}
	}()
	fmt.Printf("taskflow listening on %s (data %s)\n", cfg.HTTPAddr, cfg.DataDir)
	if err := cl.Start(); err != nil {
		log.Fatal(err)
	}
}
