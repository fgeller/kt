package main

import (
	"bytes"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func cmd(t *testing.T, name string, args ...string) (int, string, string) {
	cmd := exec.Command(name, args...)
	var stdOut, stdErr bytes.Buffer
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "KT_BROKERS=localhost:9092")
	_ = cmd.Run()
	status := cmd.ProcessState.Sys().(syscall.WaitStatus)
	return status.ExitStatus(), stdOut.String(), stdErr.String()
}

func build(t *testing.T) {
	var status int

	status, _, _ = cmd(t, "make", "build")
	require.Zero(t, status)

	status, _, _ = cmd(t, "ls", "kt")
	require.Zero(t, status)
}

func TestSystem(t *testing.T) {
	build(t)

	status, out, err := cmd(t, "./kt", "topic")
	require.Zero(t, status)
	require.Empty(t, err)
	require.JSONEq(t, `{"name": "kt-test"}`, out)
}
