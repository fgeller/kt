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
* Binary keys and payloads can be passed and presented in base64 or hex encoding
* Support for tls authentication

[![Build Status](https://travis-ci.org/fgeller/kt.svg?branch=master)](https://travis-ci.org/fgeller/kt)

## Examples

Read details about topics that match the regex `output`

    $ kt topic -filter news -partitions
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

Produce messages:

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

Or pass in JSON object to control key, value and partition:

    $ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | kt produce -topic actor-news
    {
      "count": 1,
      "partition": 0,
      "startOffset": 5
    }

Read messages at specific offsets on specific partitions:

    $ kt consume -topic actor-news -offsets 0=1:2
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

Follow a topic, starting relative to newest offset:

    $ kt consume -topic actor-news -offsets all=newest-1:
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

View offsets for a given consumer group:

    $ kt group -group enews -topic actor-news -partitions 0
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

Change consumer group offset:

    $ kt group -group enews -topic actor-news -partitions 0 -reset 1
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
    $ kt group -group enews -topic actor-news -partitions 0
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

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ go get -u github.com/fgeller/kt

### Docker

[@Paxa](https://github.com/Paxa) maintains an image to run kt in a Docker environment - thanks!

For more information: [https://github.com/Paxa/kt](https://github.com/Paxa/kt)

## Usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.
            group          consumer group information and modification.

    Use "kt [command] -help" for for information about the command.

