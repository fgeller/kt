package main

import (
	"fmt"
	"os"
	"os/signal"
)

// TODO have these all the time
var buildVersion, buildTime string

func listenForInterrupt() chan struct{} {
	closer := make(chan struct{})
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		fmt.Fprintf(os.Stderr, "received interrupt - shutting down\n")
		close(closer)
	}()

	return closer
}

type command interface {
	run(args []string, closer chan struct{})
}

func init() {
	if len(buildTime) > 0 && len(buildVersion) > 0 {
		usageMessage = fmt.Sprintf(`%v
Build %v from %v.`, usageMessage, buildVersion, buildTime)
	}
}

var usageMessage = `kt is a tool for Kafka.

Usage:

	kt command [arguments]

The commands are:

	consume    consume messages.
	produce    produce messages.
	topic      topic information.
	offset     offset information and modification
	group      consumer group information and modification

Use "kt [command] -help" for for information about the command.

More at https://github.com/fgeller/kt`

func parseArgs() command {
	if len(os.Args) < 2 {
		failf(usageMessage)
	}

	switch os.Args[1] {
	case "consume":
		return &consumeCmd{}
	case "produce":
		return &produceCmd{}
	case "topic":
		return &topicCmd{}
	case "offset":
		return &offsetCmd{}
	case "group":
		return &groupCmd{}
	default:
		failf(usageMessage)
		return nil
	}
}

func main() {
	cmd := parseArgs()
	closer := listenForInterrupt()
	cmd.run(os.Args[2:], closer)
}
