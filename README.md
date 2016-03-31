# kt - a Kafka tool

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* Display topic information (e.g., offsets per partitions)
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/).
* JSON input to facilitate use of tools like [jsonify](https://github.com/fgeller/jsonify).
* Fast start up time.
* No buffering of output.

## Usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.

    Use "kt [command] -help" for for information about the command.

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GO15VENDOREXPERIMENT=1 go get github.com/fgeller/kt
    $ GO15VENDOREXPERIMENT=1 go install github.com/fgeller/kt

## Example

Read details about topics that match the regex `output`

    $ kt topic -filter output -partitions
    {"name":"output","partitions":[{"id":0,"oldestOffset":0,"newestOffset":0}]}

Generate sample data using [jsonify](https://github.com/fgeller/jsonify):

    $ for i in {1..5}; do jsonify -ts "$(date --iso-8601=ns)" =success "$(if (($i % 2)) ; then echo 'true' ; else echo 'false' ; fi)" ; done | tee data.txt
    {"success":true,"ts":"2016-04-01T07:09:14,710388000+13:00"}
    {"success":false,"ts":"2016-04-01T07:09:14,729303000+13:00"}
    {"success":true,"ts":"2016-04-01T07:09:14,747473000+13:00"}
    {"success":false,"ts":"2016-04-01T07:09:14,762924000+13:00"}
    {"success":true,"ts":"2016-04-01T07:09:14,780448000+13:00"}

Augment sample data with details on how to send them to kafka by specifying the
key `client-1` and partition `0`. Each message is produced to the topic `output`

    $ cat data.txt | while read r; do jsonify -key "client-1" -value $r =partition 0 ; done | tee >(cat 1>&2) | kt produce -topic output
    {"key":"client-1","partition":0,"value":"{\"success\":true,\"ts\":\"2016-04-01T07:09:14,710388000+13:00\"}"}
    Sent to partition 0 at offset 0 with key client-1.
    {"key":"client-1","partition":0,"value":"{\"success\":false,\"ts\":\"2016-04-01T07:09:14,729303000+13:00\"}"}
    Sent to partition 0 at offset 1 with key client-1.
    {"key":"client-1","partition":0,"value":"{\"success\":true,\"ts\":\"2016-04-01T07:09:14,747473000+13:00\"}"}
    Sent to partition 0 at offset 2 with key client-1.
    {"key":"client-1","partition":0,"value":"{\"success\":false,\"ts\":\"2016-04-01T07:09:14,762924000+13:00\"}"}
    Sent to partition 0 at offset 3 with key client-1.
    {"key":"client-1","partition":0,"value":"{\"success\":true,\"ts\":\"2016-04-01T07:09:14,780448000+13:00\"}"}
    Sent to partition 0 at offset 4 with key client-1.

Consume all message on the topic `output` and use [jq](https://github.com/stedolan/jq)
to find the messages that have a negative `success` flag.

    $ kt consume -topic output -timeout 100ms | jq -M -c '.message |= fromjson | select(.message.success == false)'
    {"partition":0,"offset":1,"key":"client-1","message":{"success":false,"ts":"2016-04-01T07:09:14,729303000+13:00"}}
    {"partition":0,"offset":3,"key":"client-1","message":{"success":false,"ts":"2016-04-01T07:09:14,762924000+13:00"}}
    2016/04/01 07:12:27 Consuming from partition [0] timed out.
