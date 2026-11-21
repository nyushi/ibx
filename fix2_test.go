package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func requireText(t *testing.T, output string, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(output, part) {
			t.Errorf("missing %q in %q", part, output)
		}
	}
}

func TestFix2Socket(t *testing.T) {
	base := stageRoot(t)
	root := filepath.Join(base, strings.Repeat("x", 100))
	t.Setenv("IBX_ROOT", root)
	t.Setenv("IBX_SOCKET", "")
	t.Setenv("XDG_RUNTIME_DIR", base)
	t.Cleanup(func() { exec.Command(binary, "--root", root, "daemon", "stop").Run() })
	t.Run("fallback", func(t *testing.T) {
		stageRun(t, 0, "--box", "box", "send", "message")
		stageRun(t, 0, "status")
		requireText(t, stageRun(t, 0, "--box", "box", "take"), "message")
		hash := sha256.Sum256([]byte(root))
		if st, err := os.Stat(filepath.Join(base, fmt.Sprintf("ibx-%x.sock", hash[:8]))); err != nil || st.Mode()&os.ModeSocket == 0 {
			t.Fatalf("expected the hashed fallback socket: %v", err)
		}
	})
	t.Run("explicit", func(t *testing.T) {
		path := filepath.Join(root, "ibx.sock")
		t.Setenv("IBX_SOCKET", path)
		requireText(t, stageRun(t, 1, "daemon"), path, fmt.Sprint(len(path)), "sun_path", "108", "IBX_SOCKET")
		requireText(t, stageRun(t, 1, "status"), path, fmt.Sprint(len(path)), "sun_path", "108", "IBX_SOCKET")
	})
	t.Run("oversize fallback", func(t *testing.T) {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("XDG_RUNTIME_DIR", root)
		hash := sha256.Sum256([]byte(root))
		path := filepath.Join(root, fmt.Sprintf("ibx-%x.sock", hash[:8]))
		requireText(t, stageRun(t, 1, "status"), path, fmt.Sprint(len(path)), "sun_path", "108", "IBX_SOCKET")
	})
}

func TestFix2InferredStartup(t *testing.T) {
	root := stageRoot(t)
	t.Setenv("IBX_ROOT", "")
	path := filepath.Join(root, "missing", "ibx.sock")
	t.Setenv("IBX_SOCKET", path)
	out := stageRun(t, 1, "--box", filepath.Join(root, "box"), "put", "body")
	requireText(t, out, root, path, "inferred from --box", "autostart")
	eq(t, strings.Count(out, "ibx: autostart"), 1)

	// A foreign-owned socket directory exercises the ownership error before
	// bind. Keep the inferred root and its daemon log inside the test area.
	for _, dir := range []string{"/", "/tmp"} {
		st, err := os.Stat(dir)
		if err == nil && st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
			path = filepath.Join(dir, "ibx-fix2-ownership-test.sock")
			t.Setenv("IBX_SOCKET", path)
			out = stageRun(t, 1, "--box", filepath.Join(root, "box"), "put", "body")
			requireText(t, out, root, path, "inferred from --box", "socket directory must be owned by UID",
				"set IBX_ROOT or --root to a directory owned by the user")
			return
		}
	}
	t.Log("no foreign-owned socket directory found for the ownership check")
}

func TestFix2BoxRoot(t *testing.T) {
	root := stageRoot(t)
	outside := filepath.Join(t.TempDir(), "box")
	t.Run("outside", func(t *testing.T) {
		requireText(t, stageRun(t, 2, "--box", outside, "list"), outside, root)
		requireText(t, stageRun(t, 2, "send", outside, "body"), outside, root)
		requireText(t, stageRun(t, 2, "--root", root, "--box", outside, "list"), outside, root)
		t.Setenv("IBX_ROOT", "")
		requireText(t, stageRun(t, 2, "--root", root, "put", outside, "body"), outside, root)
		t.Setenv("IBX_ROOT", root)
		link := filepath.Join(root, "link")
		if err := os.Symlink(filepath.Dir(outside), link); err != nil {
			t.Fatal(err)
		}
		requireText(t, stageRun(t, 2, "--box", filepath.Join(link, "box"), "list"), filepath.Join(link, "box"), root)
		t.Setenv("IBX_BOX", outside)
		requireText(t, stageRun(t, 2, "list"), outside, root)
	})
	t.Run("inside", func(t *testing.T) {
		stageRun(t, 0, "--box", filepath.Join(root, "box"), "put", "inside")
	})
	t.Run("inferred", func(t *testing.T) {
		t.Setenv("IBX_ROOT", "")
		t.Setenv("IBX_SOCKET", "")
		t.Cleanup(func() { exec.Command(binary, "--box", outside, "daemon", "stop").Run() })
		requireText(t, stageRun(t, 0, "--box", outside, "put", "outside"), "inferred from --box")
		requireText(t, stageRun(t, 0, "--box", outside, "take"), "outside")
	})
}

func TestFix2Version(t *testing.T) {
	for _, arg := range []string{"--version", "version"} {
		t.Run(arg, func(t *testing.T) {
			eq(t, stageRun(t, 0, arg), "ibx 2.0.1\n")
		})
	}
}

func TestFix2Recovery(t *testing.T) {
	root := stageRoot(t)
	other := filepath.Join(root, "other")
	if out, err := exec.Command("go", "build", "-ldflags", "-X main.ibxVersion=other", "-o", other, ".").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	startOther := func(t *testing.T) {
		t.Helper()
		if out, err := exec.Command(other, "--box", "box", "put", "other").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
	t.Run("manual", func(t *testing.T) {
		startOther(t)
		requireText(t, stageRun(t, 1, "status"), "version mismatch", "2.0.1", "other", "run: ibx daemon restart", "IBX_AUTORESTART=1")
		stageRun(t, 0, "daemon", "restart")
		requireText(t, stageRun(t, 0, "status"), "version: 2.0.1")
	})
	t.Run("automatic", func(t *testing.T) {
		stageRun(t, 0, "daemon", "stop")
		startOther(t)
		t.Setenv("IBX_AUTORESTART", "1")
		stageRun(t, 0, "--box", "box", "send", "retried")
		requireText(t, stageRun(t, 0, "status"), "version: 2.0.1")
		requireText(t, stageRun(t, 0, "--box", "box", "list"), "retried")
	})
	t.Run("stop", func(t *testing.T) {
		stageRun(t, 0, "daemon", "stop")
		startOther(t)
		stageRun(t, 0, "daemon", "stop")
		stageRun(t, 1, "status")
	})
}

func TestFix2MismatchProtocol(t *testing.T) {
	for _, command := range []string{"status", "restart", "stop"} {
		t.Run(command, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			client.SetDeadline(time.Now().Add(time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d := daemonServer{root: t.TempDir(), stopping: ctx, shutdown: make(chan shutdownRequest)}
			go func() {
				defer server.Close()
				d.serveVerified(server, func() {})
			}()

			enc, dec := json.NewEncoder(client), json.NewDecoder(client)
			if err := enc.Encode(packet{Version: "other", Protocol: protocolVersion}); err != nil {
				t.Fatal(err)
			}
			var hello packet
			if err := dec.Decode(&hello); err != nil {
				t.Fatal(err)
			}
			requireText(t, hello.Error, "version mismatch", "other", ibxVersion)

			request := packet{Args: []string{command}, Cwd: d.root}
			if command != "status" {
				request.Args = []string{"daemon", command}
				if command == "restart" {
					request.Executable = binary
				}
			}
			if err := enc.Encode(request); err != nil {
				t.Fatal(err)
			}
			if command != "status" {
				select {
				case shutdown := <-d.shutdown:
					eq(t, shutdown.restart, command == "restart")
					eq(t, shutdown.executable, request.Executable)
					close(shutdown.done)
				case <-time.After(time.Second):
					t.Fatal("control request did not reach the shutdown handler")
				}
			}
			var reply packet
			if err := dec.Decode(&reply); err != nil {
				t.Fatal(err)
			}
			if command == "status" {
				eq(t, reply.Error, hello.Error)
			} else {
				eq(t, reply.Kind, "exit")
				eq(t, reply.Code, 0)
			}
		})
	}
}

func TestLegacyDaemonControl(t *testing.T) {
	const detail = "daemon version mismatch: client 2.0.1/1, daemon 2.0.0/1"
	const want = detail + "; the running daemon is too old to accept restart control. Stop it with its matching client (run ibx daemon stop using the daemon's version) or terminate the daemon process, then retry your original command. The new client will autostart a daemon of its own version."

	for _, command := range []string{"restart", "stop"} {
		for _, stage := range []string{"write", "read"} {
			t.Run(command+"/"+stage, func(t *testing.T) {
				client, server := net.Pipe()
				defer client.Close()
				defer server.Close()
				server.SetDeadline(time.Now().Add(time.Second))
				peerResult := make(chan error, 1)
				go func() {
					defer server.Close()
					dec := json.NewDecoder(server)
					var hello packet
					if err := dec.Decode(&hello); err != nil {
						peerResult <- err
						return
					}
					if err := json.NewEncoder(server).Encode(packet{Error: detail}); err != nil {
						peerResult <- err
						return
					}
					if stage == "read" {
						var request packet
						peerResult <- dec.Decode(&request)
						return
					}
					peerResult <- nil
				}()

				o := options{command: "daemon", sub: command}
				code, err := exchange(client, nil, o, []string{"daemon", command})
				if peerErr := <-peerResult; peerErr != nil {
					t.Fatal(peerErr)
				}
				eq(t, code, 1)
				if err == nil {
					t.Fatal("expected a legacy daemon control error")
				}
				eq(t, err.Error(), want)
			})
		}
	}
}
