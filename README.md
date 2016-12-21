# kt - a Kafka tool that likes JSON.

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* Display topic information (e.g., with partition offset and leader info)
* Modify consumer group offsets (e.g., resetting or manually setting offsets per topic and per partition)
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/).
* JSON input to facilitate automation via tools like [jsonify](https://github.com/fgeller/jsonify).
* Configure brokers and topic via environment variables for a shell session
* Fast start up time.
* No buffering of output.

[![Build Status](https://travis-ci.org/fgeller/kt.svg?branch=master)](https://travis-ci.org/fgeller/kt)

## Examples

Read details about topics that match the regex `output`

    $ kt topic -filter news -partitions
    {"name":"actor-news","partitions":[{"id":0,"oldest":0,"newest":0}]}

Produce messages:

    $ echo 'Alice wins Oscar' | kt produce -topic actor-news -literal
    {"partition": 0, "startOffset": 0, "count": 1}
    $ for i in {6..9} ; do echo Bourne sequel $i in production. | kt produce -topic actor-news -literal ; done
    {"partition": 0, "startOffset": 1, "count": 1}
    {"partition": 0, "startOffset": 2, "count": 1}
    {"partition": 0, "startOffset": 3, "count": 1}
    {"partition": 0, "startOffset": 4, "count": 1}

Or pass in JSON object to control key, value and partition:

    $ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | kt produce -topic actor-news
    {"partition": 0, "startOffset": 5, "count": 1}

Read messages at specific offsets on specific partitions:

    $ kt consume -topic actor-news -offsets 0=1:2
    {"partition":0,"offset":1,"key":"","value":"Bourne sequel 6 in production."}
    {"partition":0,"offset":2,"key":"","value":"Bourne sequel 7 in production."}

Follow a topic, starting relative to newest offset:

    $ kt consume -topic actor-news -offsets all=newest-1:
    {"partition":0,"offset":3,"key":"","value":"Bourne sequel 8 in production."}
    {"partition":0,"offset":4,"key":"","value":"Bourne sequel 9 in production."}
    {"partition":0,"offset":5,"key":"Arni","value":"Terminator terminated"}
    ^Creceived interrupt - shutting down
    shutting down partition consumer for partition 0

View offsets for a given consumer group:

    $ kt offset -topic actor-news -group enews
    {"consumer-group":"enews","topic":"actor-news","partition":0,"partition-offset":5,"consumer-offset":-1}

Change consumer group offset:

    $ kt offset -topic actor-news -group enews -setConsumerOffsets 1
    $ kt offset -topic actor-news -group enews
    {"consumer-group":"enews","topic":"actor-news","partition":0,"partition-offset":5,"consumer-offset":1}

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ go get -u github.com/fgeller/kt

## Usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.
            offset         offset information and modification.

    Use "kt [command] -help" for for information about the command.

