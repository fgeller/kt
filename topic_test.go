package main

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestTopicParseArgsUsesEnvVar(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_BROKERS", "hans:2000")

	cmd0, _, err := parseCmd("hkt", "topic")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*topicCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"hans:2000"})
}

// brokers default to localhost:9092
func TestTopicParseArgsDefault(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_BROKERS", "")

	cmd0, _, err := parseCmd("hkt", "topic")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*topicCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"localhost:9092"})
}

func TestTopicParseArgsFlagsOverrideEnv(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	// command line arg wins
	c.Setenv("KT_BROKERS", "BLABB")

	cmd0, _, err := parseCmd("hkt", "topic", "-brokers", "hans:2000")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*topicCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"hans:2000"})
}
