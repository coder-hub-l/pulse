package main

import (
	"fmt"
	"log"
	"os"

	"pulse/internal/config"
)

func main() {
	cfgPath := os.Getenv("PULSE_CONFIG")
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	log.Printf("Pulse starting on port %d", cfg.Server.Port)
	log.Printf("Postgres: %s", cfg.Postgres.DSN)
	log.Printf("Redis: %s", cfg.Redis.Addr)

	// Print resolved settings for configured metrics to verify merge logic.
	for key := range cfg.Metrics {
		s := cfg.ForMetric(key)
		log.Printf("Metric %q: window=%s, alpha=%.2f, z=%.1f, heartbeat=%s, cooldown=%s",
			key, s.WindowSize, s.EWMAAlpha, s.ZScoreThreshold, s.HeartbeatTimeout, s.AlertCooldown)
	}

	// TODO: Step 2 will replace this with the HTTP server.
	fmt.Println("Config loaded successfully. HTTP server coming in step 2.")
}
