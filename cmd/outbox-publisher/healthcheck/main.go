// Command healthcheck probes the outbox-publisher liveness endpoint from
// inside the container. It is built into the minimal runtime image so no HTTP
// client tools are needed there.
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

const defaultURL = "http://127.0.0.1:8081/health"

func main() {
	url := os.Getenv("OUTBOX_PUBLISHER_HEALTH_URL")
	if url == "" {
		url = defaultURL
	}

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck request failed: %v\n", err)
		os.Exit(1)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck status = %d, want 200\n", response.StatusCode)
		os.Exit(1)
	}
}
