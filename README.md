# mongo db migration

use MongoDB changestreams and MongoDB oplog to replicate changes from a source MongoDB instance to a destination.

## mongodb-generator

python app uses mimesis to generate some documents, and some random events every 15 seconds

## mongodb-listener

listens on a change stream in a mongodb source replicaset, publishes the changes to a pubsub topic on GCP for buffering/retry

## mongodb-replicator

listens on the pubsub topic using a pull subscription, writes the records to a destination mongodb replicaset


## TODO

- add instructions on how to create mongodb source/destinations - create data disk, initialize replicaset, size oplog, etc
- proper IAM, authentication on mongodb
- filter mongodb documents on a key (for example, sending a subset of mongodb documents)
  - to simulate sharding out a tenant in a multi-tenant database
- test update/delete of documents