# kt - a Kafka tool

Some reasons why you might be interested:

* Fast start up time.
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/)
* Consume messages between specific offsets.
* No buffering of output.
* Display topic information (e.g., offsets per partitions)

## Example usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.

    Use "kt [command] -help" for for information about the command.

### consume

    $ kt consume -help
    Usage of consume:
      -brokers string
            Comma separated list of brokers. Port defaults to 9092 when omitted. (default "localhost:9092")
      -offsets string
            Colon separated offsets where to start and end reading messages.
      -timeout duration
            Timeout after not reading messages (default 0 to disable).
      -topic string
            Topic to consume.

    $ kt consume -topic greetings
    {"partition":0,"offset":0,"key":"id-23","message":"ola"}
    {"partition":0,"offset":1,"key":"hello.","message":"hello."}
    {"partition":0,"offset":2,"key":"bonjour.","message":"bonjour."}
    ^C2016/03/30 16:00:25 Received interrupt - shutting down...

    $ kt consume -topic greetings -timeout 1s
    {"partition":0,"offset":0,"key":"id-23","message":"ola"}
    {"partition":0,"offset":1,"key":"hello.","message":"hello."}
    {"partition":0,"offset":2,"key":"bonjour.","message":"bonjour."}
    2016/03/30 16:01:04 Consuming from partition [0] timed out.

    $ kt consume -topic greetings -timeout 50ms -offsets 1:
    {"partition":0,"offset":1,"key":"hello.","message":"hello."}
    {"partition":0,"offset":2,"key":"bonjour.","message":"bonjour."}
    2016/03/30 16:01:30 Consuming from partition [0] timed out.

    $ kt consume -topic greetings -timeout 50ms -offsets 1:1
    {"partition":0,"offset":1,"key":"hello.","message":"hello."}

### produce

    $ kt produce -help
    Usage of produce:
      -brokers string
            Comma separated list of brokers. Port defaults to 9092 when omitted. (default "localhost:9092")
      -topic string
            Topic to produce to.

    $ echo '{"key": "id-23", "value": "ola"}' | kt produce -topic greetings
    Sent message to partition 0 at offset 3.

    $ kt consume -topic greetings -json -timeout 1s -offsets 3:
    {"partition":0,"offset":3,"key":"id-23","message":"ola"}

    $ kt produce -topic greetings
    hello.
    Sent message to partition 0 at offset 4.
    bonjour.
    Sent message to partition 0 at offset 5.

    $ kt consume -topic greetings -json -timeout 1s -offsets 4:
    {"partition":0,"offset":4,"key":"hello.","message":"hello."}
    {"partition":0,"offset":5,"key":"bonjour.","message":"bonjour."}

### topic

    $ kt topic -help
    Usage of topic:
      -brokers string
            Comma separated list of brokers. Port defaults to 9092 when omitted. (default "localhost:9092")
      -filter string
            Regex to filter topics by name.
      -leaders
            Include leader information per partition.
      -partitions
            Include information per partition.
      -replicas
            Include replica ids per partition.

    $ kt topic
    {"name":"__consumer_offsets"}
    {"name":"kt-test"}
    {"name":"test"}

    $ kt topic -filter test
    {"name":"kt-test"}
    {"name":"test"}

    $ kt topic -filter test -partitions
    {"name":"test","partitions":[{"id":0,"oldestOffset":3,"newestOffset":5}]}
    {"name":"kt-test","partitions":[{"id":0,"oldestOffset":7,"newestOffset":37}]}

    $ kt topic -filter test -partitions -replicas -leaders
    {"name":"kt-test","partitions":[{"id":0,"oldestOffset":7,"newestOffset":37,"leader":"bert:9092","replicas":[0]}]}
    {"name":"test","partitions":[{"id":0,"oldestOffset":3,"newestOffset":5,"leader":"bert:9092","replicas":[0]}]}

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GO15VENDOREXPERIMENT=1 go get github.com/fgeller/kt
    $ GO15VENDOREXPERIMENT=1 go install github.com/fgeller/kt
