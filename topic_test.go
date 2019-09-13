package main

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestTopicParseArgsUsesEnvVar(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_BROKERS", "hans:2000")

	var target topicCmd
	target.parseArgs(nil)
	c.Assert(target.brokers, qt.DeepEquals, []string{"hans:2000"})
}

// brokers default to localhost:9092
func TestTopicParseArgsDefault(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv("KT_BROKERS", "")

	var target topicCmd
	target.parseArgs(nil)
	c.Assert(target.brokers, qt.DeepEquals, []string{"localhost:9092"})
}

func TestTopicParseArgsFlagsOverrideEnv(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	// command line arg wins
	c.Setenv("KT_BROKERS", "BLABB")

	var target topicCmd
	target.parseArgs([]string{"-brokers", "hans:2000"})
	c.Assert(target.brokers, qt.DeepEquals, []string{"hans:2000"})
}
