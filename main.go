package main

import (
	"fmt"
	"os"
)

const AppVersion = "v14.0.0-pre"

var buildVersion, buildTime string

var versionMessage = fmt.Sprintf(`kt version %s`, AppVersion)

func init() {
	if buildVersion == "" && buildTime == "" {
		return
	}

	versionMessage += " ("
	if buildVersion != "" {
		versionMessage += buildVersion
	}

	if buildTime != "" {
		if buildVersion != "" {
			versionMessage += " @ "
		}
		versionMessage += buildTime
	}
	versionMessage += ")"
}

var usageMessage = fmt.Sprintf(`kt is a tool for Kafka.

Usage:

	kt command [arguments]

The commands are:

	consume    consume messages.
	produce    produce messages.
	topic      topic information.
	group      consumer group information and modification.
	admin      basic cluster administration.

Use "kt [command] -help" for more information about the command.

Use "kt -version" for details on what version you are running.

Authentication:

Authentication with Kafka can be configured via a JSON file.
You can set the file name via an "-auth" flag to each command or
set it via the environment variable %s.

You can find more details at https://github.com/fgeller/kt

%s`, ENV_AUTH, versionMessage)

func main() {
	if len(os.Args) < 2 {
		failf(usageMessage)
	}

	var cmd command
	switch os.Args[1] {
	case "consume":
		cmd = &consumeCmd{}
	case "produce":
		cmd = &produceCmd{}
	case "topic":
		cmd = &topicCmd{}
	case "group":
		cmd = &groupCmd{}
	case "admin":
		cmd = &adminCmd{}
	case "-h", "-help", "--help":
		quitf(usageMessage)
	case "-version", "--version":
		quitf(versionMessage)
	default:
		failf(usageMessage)
	}

	cmd.run(os.Args[2:])
}
