package s3testing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"
)

const (
	AdminUserOrPassword = "simpleblob"
)

// ServeMinio starts a Minio server in the background,
// and waits for it to be ready.
// It returns its address,
// and a function to stop the server gracefully.
//
// The admin username and password for the server are both "simpleblob".
//
// If the minio binary cannot be found locally,
// it is downloaded by calling MinioBin.
func ServeMinio(ctx context.Context, dir string) (string, func() error, error) {
	port, err := FreePort()
	if err != nil {
		return "", nil, err
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cmdname, err := MinioBin()
	if err != nil {
		return "", nil, err
	}

	cmd := exec.CommandContext(ctx, cmdname, "server", "--quiet", "--address", addr, dir)
	cmd.Env = append(os.Environ(), "MINIO_BROWSER=off")
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

// minioExecPath is the cached path to minio binary,
// that is returned when calling MinioBin.
var minioExecPath string

// HasLocalMinio returns true if minio can be found in PATH.
// If an error occurs while searching,
// the function panics.
func HasLocalMinio() bool {
	_, err := exec.LookPath("minio")
	if err != nil && !errors.Is(err, exec.ErrNotFound) {
		panic(err)
	}
	return err == nil
}

// MinioBin tries to find minio in PATH,
// or downloads it to a temporary file.
//
// The result is cached and shared among callers.
func MinioBin() (string, error) {
	if minioExecPath != "" {
		return minioExecPath, nil
	}

	binpath, err := exec.LookPath("minio")
	if err == nil {
		minioExecPath = binpath
		return binpath, nil
	}
	if !errors.Is(err, exec.ErrNotFound) {
		return "", err
	}

	f, err := os.CreateTemp("", "minio")
	if err != nil {
		return "", err
	}
	defer f.Close()

	url := fmt.Sprintf("https://dl.min.io/server/minio/release/%s-%s/minio", runtime.GOOS, runtime.GOARCH)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return "", err
	}

	err = f.Chmod(0755)
	if err != nil {
		return "", err
	}

	minioExecPath = f.Name()
	return minioExecPath, nil
}
