# Quick Kafka Demo in Node JS

## What this is

We're building a sample application that tracks changes to 'users' over time.

## Getting Started

Minikube gets kind of slow and hard to configure some things. Honestly, docker-compose and confluent's own cp-docker-images repo didn't work that well either. Easiest solution was to [download the platform](https://www.confluent.io/download/) and then run `bin/confluent start` after unpacking it.

A UI will be available at this point at localhost:9021 to inspect topics, run ksql, and all kinds of things.

## Key Terms and Concepts

### Log Compaction

[Log compaction](http://kafka.apache.org/documentation.html#compaction) enables retention of keyed messages such that
> Kafka will always retain at least the last known value for each message key within the log of data for a single topic partition.
We'll want this to make sure each user will always have at least one value, but allow us to have a limited retention period for the full dataset.

## Doing Stuff

### Create Some Data

A seed file to create some things for users is provided at `src/seed/users.ts`. It can be run via ts-node, or with `npm run seed:users -- --help`. For instance, to create the 'users' topic, run `npm run seed:users -- --create-topic`, to add 10 fake users (with intentional id collisions): `npm run seed:users -- --insert`.

## Deleting a Topic

Can't be done with kafka-node, so you have to run
```sh
<confluent-path>/bin/kafka-topics --zookeeper localhost:2181 --delete --topic users
```
to drop the 'users' topic, for instance.
