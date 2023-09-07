package s3testing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"
)

const (
	AdminUserOrPassword = "simpleblob"
)

var ErrMinioNotFound = exec.ErrNotFound

// ServeMinio starts a Minio server in the background,
// and waits for it to be ready.
// It returns its address,
// and a function to stop the server gracefully.
//
// The admin username and password for the server are both "simpleblob".
//
// If the minio binary cannot be found in PATH,
// ErrMinioNotFound will be returned.
func ServeMinio(ctx context.Context, dir string) (string, func() error, error) {
	cmdname, err := exec.LookPath("minio")
	if err != nil {
		return "", nil, err
	}

	port, err := FreePort()
	if err != nil {
		return "", nil, err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cmd := exec.CommandContext(ctx, cmdname, "server", "--quiet", "--address", addr, dir)
	cmd.Env = append([]string{}, os.Environ()...)
	cmd.Env = append(cmd.Env, "MINIO_BROWSER=off")
	cmd.Env = append(cmd.Env, "MINIO_ROOT_USER="+AdminUserOrPassword, "MINIO_ROOT_PASSWORD="+AdminUserOrPassword)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", nil, err
	}

	// Wait for server to accept requests.
	readyURL := "http://" + addr + "/minio/health/ready"
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		case <-ticker.C:
		}

		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == 200 {
			break
		}
	}

	stop := func() error {
		_ = cmd.Process.Signal(os.Interrupt)
		return cmd.Wait()
	}
	return addr, stop, nil
}
