package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
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

	strOut := stdOut.String()
	strErr := stdErr.String()

	return status.ExitStatus(), strOut, strErr

}

func build(t *testing.T) {
	c := qt.New(t)

	status, _, _ := newCmd().run("make", "build")
	c.Assert(status, qt.Equals, 0)

	status, _, _ = newCmd().run("ls", "kt")
	c.Assert(status, qt.Equals, 0)
}

func TestSystem(t *testing.T) {
	c := qt.New(t)
	build(t)

	//
	// kt admin -createtopic
	//
	topicName := fmt.Sprintf("kt-test-%v", randomString(6))
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	buf, err := json.Marshal(topicDetail)
	c.Assert(err, qt.Equals, nil)
	fnTopicDetail := fmt.Sprintf("topic-detail-%v.json", randomString(6))
	err = ioutil.WriteFile(fnTopicDetail, buf, 0666)
	c.Assert(err, qt.Equals, nil)
	defer os.RemoveAll(fnTopicDetail)

	status, stdOut, stdErr := newCmd().stdIn(string(buf)).run("./kt", "admin", "-createtopic", topicName, "-topicdetail", fnTopicDetail)
	c.Logf(">> system test kt admin -createtopic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt admin -createtopic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Equals, "")

	c.Logf(">> ✓\n")
	//
	// kt produce
	//

	req := map[string]interface{}{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       "boom",
		"partition": float64(0),
	}
	buf, err = json.Marshal(req)
	c.Assert(err, qt.Equals, nil)
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "produce", "-topic", topicName)
	c.Logf(">> system test kt produce -topic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt produce -topic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Equals, "")

	var produceMessage map[string]int
	err = json.Unmarshal([]byte(stdOut), &produceMessage)
	c.Assert(err, qt.Equals, nil)
	c.Assert(produceMessage["count"], qt.Equals, 1)
	c.Assert(produceMessage["partition"], qt.Equals, 0)
	c.Assert(produceMessage["startOffset"], qt.Equals, 0)

	c.Logf(">> ✓\n")
	//
	// kt consume
	//

	status, stdOut, stdErr = newCmd().run("./kt", "consume", "-topic", topicName, "-timeout", "500ms", "-group", "hans")
	c.Logf(">> system test kt consume -topic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt consume -topic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)

	lines := splitLines(stdOut)
	c.Assert(lines, qt.Not(qt.HasLen), 0)

	var lastConsumed map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-1]), &lastConsumed)
	c.Assert(err, qt.Equals, nil, qt.Commentf("stdout: %q", stdOut))
	c.Assert(lastConsumed["value"], qt.Equals, req["value"])
	c.Assert(lastConsumed["key"], qt.Equals, req["key"])
	c.Assert(lastConsumed["partition"], qt.Equals, req["partition"])
	c.Assert(lastConsumed["timestamp"], qt.Matches, ".+")

	pt, err := time.Parse(time.RFC3339, lastConsumed["timestamp"].(string))
	c.Assert(err, qt.Equals, nil)
	if want := time.Now().Add(-2 * time.Minute); pt.Before(want) {
		c.Fatalf("timestamp is too early; got %v want after %v", pt, want)
	}

	c.Logf(">> ✓\n")
	//
	// kt group
	//

	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", topicName)
	c.Logf(">> system test kt group -topic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt group -topic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Contains, fmt.Sprintf(`found partitions=[0] for topic=%v`, topicName))
	c.Assert(stdOut, qt.Contains, fmt.Sprintf(`{"name":"hans","topic":"%v","offsets":[{"partition":0,"offset":1,"lag":0}]}`, topicName))

	c.Logf(">> ✓\n")
	//
	// kt produce
	//

	req = map[string]interface{}{
		"value":     fmt.Sprintf("hello, %s", randomString(6)),
		"key":       "boom",
		"partition": float64(0),
	}
	buf, err = json.Marshal(req)
	c.Assert(err, qt.Equals, nil)
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "produce", "-topic", topicName)
	c.Logf(">> system test kt produce -topic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt produce -topic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Equals, "")

	err = json.Unmarshal([]byte(stdOut), &produceMessage)
	c.Assert(err, qt.Equals, nil)
	c.Assert(produceMessage["count"], qt.Equals, 1)
	c.Assert(produceMessage["partition"], qt.Equals, 0)
	c.Assert(produceMessage["startOffset"], qt.Equals, 1)

	c.Logf(">> ✓\n")
	//
	// kt consume
	//

	status, stdOut, stdErr = newCmd().run("./kt", "consume", "-topic", topicName, "-offsets", "all=resume", "-timeout", "500ms", "-group", "hans")
	c.Logf(">> system test kt consume -topic %v -offsets all=resume stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt consume -topic %v -offsets all=resume stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)

	lines = splitLines(stdOut)
	c.Assert(lines, qt.HasLen, 1)

	err = json.Unmarshal([]byte(lines[len(lines)-1]), &lastConsumed)
	c.Assert(err, qt.Equals, nil)

	c.Assert(lastConsumed["value"], qt.Equals, req["value"])
	c.Assert(lastConsumed["key"], qt.Equals, req["key"])
	c.Assert(lastConsumed["partition"], qt.Equals, req["partition"])
	c.Assert(lastConsumed["timestamp"], qt.Matches, ".+")

	pt, err = time.Parse(time.RFC3339, lastConsumed["timestamp"].(string))
	c.Assert(err, qt.Equals, nil)
	if want := time.Now().Add(-2 * time.Minute); pt.Before(want) {
		c.Fatalf("timestamp is too early; got %v want after %v", pt, want)
	}

	c.Logf(">> ✓\n")
	//
	// kt group reset
	//

	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", topicName, "-partitions", "0", "-group", "hans", "-reset", "0")
	c.Logf(">> system test kt group -topic %v -partitions 0 -group hans -reset 0 stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt group -topic %v -partitions 0 -group hans -reset 0  stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)

	lines = splitLines(stdOut)
	c.Assert(lines, qt.Not(qt.HasLen), 0)

	var groupReset map[string]interface{}
	err = json.Unmarshal([]byte(lines[len(lines)-1]), &groupReset)
	c.Assert(err, qt.Equals, nil)

	c.Assert(groupReset["name"], qt.Equals, "hans")
	c.Assert(groupReset["topic"], qt.Equals, topicName)
	c.Assert(groupReset["offsets"], qt.HasLen, 1)
	offsets := groupReset["offsets"].([]interface{})[0].(map[string]interface{})
	c.Assert(offsets["partition"], qt.Equals, 0.0)
	c.Assert(offsets["offset"], qt.Equals, 0.0)

	c.Logf(">> ✓\n")
	//
	// kt group
	//

	status, stdOut, stdErr = newCmd().run("./kt", "group", "-topic", topicName)
	c.Logf(">> system test kt group -topic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt group -topic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Contains, fmt.Sprintf("found partitions=[0] for topic=%v", topicName))
	c.Assert(stdOut, qt.Contains, fmt.Sprintf(`{"name":"hans","topic":"%v","offsets":[{"partition":0,"offset":0,"lag":2}]}`, topicName))

	c.Logf(">> ✓\n")
	//
	// kt topic
	//

	status, stdOut, stdErr = newCmd().run("./kt", "topic", "-filter", topicName)
	c.Logf(">> system test kt topic stdout:\n%s\n", stdOut)
	c.Logf(">> system test kt topic stderr:\n%s\n", stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Equals, "")

	lines = splitLines(stdOut)
	c.Assert(lines, qt.Not(qt.HasLen), 0)

	expectedLines := []string{
		fmt.Sprintf(`{"name":"%v"}`, topicName),
	}
	sort.Strings(lines)
	sort.Strings(expectedLines)

	for i, l := range lines {
		c.Assert(l, qt.Equals, expectedLines[i])
	}
	c.Logf(">> ✓\n")

	//
	// kt admin -deletetopic
	//
	status, stdOut, stdErr = newCmd().stdIn(string(buf)).run("./kt", "admin", "-deletetopic", topicName)
	c.Logf(">> system test kt admin -deletetopic %v stdout:\n%s\n", topicName, stdOut)
	c.Logf(">> system test kt admin -deletetopic %v stderr:\n%s\n", topicName, stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Equals, "")

	c.Logf(">> ✓\n")

	//
	// kt topic
	//

	status, stdOut, stdErr = newCmd().run("./kt", "topic", "-filter", topicName)
	c.Logf(">> system test kt topic stdout:\n%s\n", stdOut)
	c.Logf(">> system test kt topic stderr:\n%s\n", stdErr)
	c.Assert(status, qt.Equals, 0)
	c.Assert(stdErr, qt.Equals, "")
	c.Assert(stdOut, qt.Equals, "")

	c.Logf(">> ✓\n")
}

func splitLines(s string) []string {
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}
