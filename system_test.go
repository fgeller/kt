package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cmd struct {
	in string
}

func newCmd() *cmd                  { return &cmd{} }
func (c *cmd) stdIn(in string) *cmd { c.in = in; return c }
func (c *cmd) run(name string, args ...string) (int, string, string) {
	cmd := exec.Command(name, args...)

	var stdOut, stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "KT_BROKERS=localhost:9092")

	if len(c.in) > 0 {
		cmd.Stdin = strings.NewReader(c.in)
	}

	_ = cmd.Run()
	status := cmd.ProcessState.Sys().(syscall.WaitStatus)
	return status.ExitStatus(), stdOut.String(), stdErr.String()

}

func build(t *testing.T) {
	var status int

	status, _, _ = newCmd().run("make", "build")
	require.Zero(t, status)

	status, _, _ = newCmd().run("ls", "kt")
	require.Zero(t, status)
}

func TestSystem(t *testing.T) {
	build(t)

	var err error
	var status int
	var stdOut, stdErr string

	status, stdOut, stdErr = newCmd().run("./kt", "topic")
	require.Zero(t, status)
	require.Empty(t, stdErr)
	require.JSONEq(t, `{"name": "kt-test"}`, stdOut)

	req := map[string]interface{}{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       "boom",
		"partition": float64(0),
	}
	buf, err := json.Marshal(req)
	require.NoError(t, err)
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "produce", "-topic", "kt-test")
	require.Zero(t, status)
	require.Empty(t, stdErr)

	var produceMessage map[string]int
	err = json.Unmarshal([]byte(stdOut), &produceMessage)
	require.NoError(t, err)
	require.Equal(t, 1, produceMessage["count"])
	require.Equal(t, 0, produceMessage["partition"])
	// ignoring startOffset

	status, stdOut, stdErr = newCmd().run("./kt", "consume", "-topic", "kt-test", "-timeout", "10ms")
	require.Zero(t, status)

	lines := strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var lastConsumed map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-2]), &lastConsumed)
	require.NoError(t, err)
	require.Equal(t, req["value"], lastConsumed["value"])
	require.Equal(t, req["key"], lastConsumed["key"])
	require.Equal(t, req["partition"], lastConsumed["partition"])
}

func randomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, length)
	r.Read(buf)
	return fmt.Sprintf("%x", buf)[:length]
}
