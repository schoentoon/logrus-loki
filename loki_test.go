package logrusloki

import (
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	lokiEndPoint  = "http://localhost:3100/api/prom/push"
	lokiBatchSize = 1024
	lokiBatchWait = 3
)

func TestLogger(t *testing.T) {
	loki, err = NewLoki(lokiEndPoint, lokiBatchSize, lokiBatchWait)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to new loki, %v\n", err)
	}

	time.Sleep(time.Second * 5)
}
