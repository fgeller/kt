# hkt - a Kafka tool that likes JSON [![Build Status](https://travis-ci.org/fgeller/kt.svg?branch=master)](https://travis-ci.org/fgeller/kt)

_This is a fork of KT that we maintain, adding features oriented toward debugging_

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* Display topic information (e.g., with partition offset and leader info).
* Modify consumer group offsets (e.g., resetting or manually setting offsets per topic and per partition).
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/).
* JSON input to facilitate automation via tools like [jsonify](https://github.com/fgeller/jsonify).
* Configure brokers with the `KT_BROKERS` environment variable.
* Fast start up time.
* No buffering of output.
* Binary keys and payloads can be passed and presented in base64 or hex encoding.
* Support for TLS authentication.
* Basic cluster admin functions: Create & delete topics.

## Examples

<details><summary>Read details about topics that match a regex</summary>

```sh
$ hkt topic -filter news -partitions
{
  "name": "actor-news",
  "partitions": [
    {
      "id": 0,
      "oldest": 0,
      "newest": 0
    }
  ]
}
```
</details>

<details><summary>Produce messages</summary>

```sh
$ echo 'Alice wins Oscar' | kt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ echo 'Bob wins Oscar' | kt produce -tlsca myca.pem -tlscert myclientcert.pem -tlscertkey mycertkey.pem -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ for i in {6..9} ; do echo Bourne sequel $i in production. | kt produce -topic actor-news -literal ;done
{
  "count": 1,
  "partition": 0,
  "startOffset": 1
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 2
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 3
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 4
}
```
</details>

<details><summary>Or pass in JSON object to control key, value and partition</summary>

```sh
$ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | hkt produce -topic actor-news
{
  "count": 1,
  "partition": 0,
  "startOffset": 5
}
```
</details>

<details><summary>Read messages at specific offsets on specific partitions</summary>

```sh
$ hkt consume -topic actor-news -offsets 0=1:2
{
  "partition": 0,
  "offset": 1,
  "key": "",
  "value": "Bourne sequel 6 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 2,
  "key": "",
  "value": "Bourne sequel 7 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
```
</details>

<details><summary>Follow a topic, starting relative to newest offset</summary>

```sh
$ hkt consume -topic actor-news -offsets all=newest-1:
{
  "partition": 0,
  "offset": 4,
  "key": "",
  "value": "Bourne sequel 9 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 5,
  "key": "Arni",
  "value": "Terminator terminated",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
^Creceived interrupt - shutting down
shutting down partition consumer for partition 0
```
</details>

<details><summary>View offsets for a given consumer group</summary>

```sh
$ hkt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 6,
      "lag": 0
    }
  ]
}
```
</details>

<details><summary>Change consumer group offset</summary>

```sh
$ hkt group -group enews -topic actor-news -partitions 0 -reset 1
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
$ hkt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
```
</details>

<details><summary>Create and delete a topic</summary>

```sh
$ hkt admin -createtopic morenews -topicdetail <(jsonify =NumPartitions 1 =ReplicationFactor 1)
$ hkt topic -filter news
{
  "name": "morenews"
}
$ hkt admin -deletetopic morenews
$ hkt topic -filter news
```

</details>

<details><summary>Change broker address via environment variable</summary>

```sh
$ export KT_BROKERS=brokers.kafka:9092
$ hkt <command> <option>
```

</details>

## Installation

You can download hkt via the [Releases](https://github.com/heetch/hkt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GOMODULE111=off go get -u github.com/heetch/hkt

## Usage:

    $ hkt -help
    hkt is a tool for Kafka.

    Usage:

            hkt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.
            group          consumer group information and modification.
            admin          basic cluster administration.

    Use "hkt [command] -help" for for information about the command.
