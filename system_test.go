package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"testing"

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

	//
	// kt produce
	//

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

	//
	// kt consume
	//

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

	//
	// kt group
	//

	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", "kt-test")
	require.Zero(t, status)
	require.Contains(t, stdErr, "found partitions=[0] for topic=kt-test")
	require.Empty(t, stdOut)

	//
	// kt group reset
	//

	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", "kt-test", "-partitions", "0", "-group", "hans", "-reset", "1")
	require.Zero(t, status)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 1)

	var groupReset map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-2]), &groupReset)
	require.NoError(t, err)

	require.Equal(t, groupReset["name"], "hans")
	require.Equal(t, groupReset["topic"], "kt-test")
	require.Len(t, groupReset["offsets"], 1)
	offsets := groupReset["offsets"].([]interface{})[0].(map[string]interface{})
	require.Equal(t, offsets["partition"], float64(0))
	require.Equal(t, offsets["offset"], float64(1))

	//
	// kt topic
	//

	status, stdOut, stdErr = newCmd().run("./kt", "topic")
	require.Zero(t, status)
	require.Empty(t, stdErr)

	lines = strings.Split(stdOut, "\n")
	require.True(t, len(lines) > 0)

	expectedLines := []string{
		`{"name": "kt-test"}`,
		`{"name": "__consumer_offsets"}`,
	}
	sort.Strings(lines)
	sort.Strings(expectedLines)

	for i, l := range lines {
		if l == "" { // final newline
			continue
		}
		require.JSONEq(t, expectedLines[i-1], l, fmt.Sprintf("line %i", i-1))
	}
}
