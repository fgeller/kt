package main

import (
	"fmt"
	"os"
)

// TODO have these all the time
var buildVersion, buildTime string

type command interface {
	run(args []string)
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
	group      consumer group information and modification.
	admin      basic cluster administration.

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
	case "group":
		return &groupCmd{}
	case "admin":
		return &adminCmd{}
	case "-h", "-help", "--help":
		quitf(usageMessage)
	default:
		failf(usageMessage)
	}
	return nil
}

func main() {
	cmd := parseArgs()
	cmd.run(os.Args[2:])
}
