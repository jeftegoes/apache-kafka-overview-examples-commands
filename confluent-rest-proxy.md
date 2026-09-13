# Confluent REST Proxy <!-- omit in toc -->

## Contents <!-- omit in toc -->

- [1. Introduction](#1-introduction)
- [2. Making a request to the REST Proxy](#2-making-a-request-to-the-rest-proxy)
  - [2.1. Formats](#21-formats)
- [3. Topic Operations](#3-topic-operations)
- [4. Producing with the REST Proxy](#4-producing-with-the-rest-proxy)
  - [4.1. Producing with the REST Proxy - Binary](#41-producing-with-the-rest-proxy---binary)
    - [4.1.1. Consuming with the REST Proxy](#411-consuming-with-the-rest-proxy)
    - [4.1.2. Consuming with the REST Proxy - Binary](#412-consuming-with-the-rest-proxy---binary)
  - [4.2. Producing with the REST Proxy - JSON](#42-producing-with-the-rest-proxy---json)
    - [4.2.1. Consuming with the REST Proxy - JSON](#421-consuming-with-the-rest-proxy---json)
  - [4.3. Producing with the REST Proxy - Avro](#43-producing-with-the-rest-proxy---avro)
    - [4.3.1. Consuming with the REST Proxy - Avro](#431-consuming-with-the-rest-proxy---avro)

# 1. Introduction

- Kafka is great for Java based consumers / producers, but sometimes clients are looking for other languages.
- Although things are getting better.
- Additionally, sometimes Avro support for some languages isn't great, whereas JSON / HTTP requests are great.
- For all these reasons, Confluent created the REST Proxy.
  - It's an open source project created by Confluent.
- It's integrated with the schema registry so that consumers and producers can easily read and write to Avro topics.
- There's a performance hit to using HTTP instead of Kafka's native protocol and it's been estimated that the throughput decrease is 3–4x.
- It's up to the producing application to batch events.
- The Confluent REST Proxy is already installed on our Docker Kafka Cluster!
  ![Confluent REST Proxy](/images/confluent-rest-proxy.png)

# 2. Making a request to the REST Proxy

- Content Type has to be specified in a header (plus an Accept header).
  ![Making a request to the REST Proxy](/images/making-request-rest-proxy.png)
- **Example**
  - `Content-Type: application/vnd.kafka.avro.v2+json`
  - `Accept: application/vnd.kafka.avro.v2+json`

## 2.1. Formats

| Format     | `Content-Type`                         | `Accept`                               |
| ---------- | -------------------------------------- | -------------------------------------- |
| **Binary** | `application/vnd.kafka.binary.v2+json` | `application/vnd.kafka.binary.v2+json` |
| **JSON**   | `application/vnd.kafka.json.v2+json`   | `application/vnd.kafka.json.v2+json`   |
| **Avro**   | `application/vnd.kafka.avro.v2+json`   | `application/vnd.kafka.avro.v2+json`   |

# 3. Topic Operations

- Getting a list of topics (GET /topics).
- Getting a specific topic (GET /topics/topic_name).
- We can't create / configure a topic with the REST Proxy.
- It has to be done outside of the REST Proxy.

# 4. Producing with the REST Proxy

- We have 3 choices with the REST Proxy to produce data:
  - `Binary` (raw bytes encoded with base64).
  - `JSON` (plain json).
  - `Avro` (JSON encoded).
- [When in doubt, always refer to the documentation](https://docs.confluent.io/platform/current/kafka-rest/index.html)
- We can do batching in the produce request.

## 4.1. Producing with the REST Proxy - Binary

- Binary data has to be base64 encoded before sending it off to Kafka.
- `Base64` is a smart way invented to safely transfer bytes over the internet in a way that won't break protocols (only using 64 different characters).
- This way, any binary array can be sent (image data, string with weird characters, etc.).
- There are many libraries for each language to base64 encode binary data.
- We can learn more about `base64` here: [Base64](https://en.wikipedia.org/wiki/Base64).

### 4.1.1. Consuming with the REST Proxy

- To consume with the REST Proxy, we first need to create a consumer in a specific consumer group.
- Once we open a consumer, the REST Proxy returns a URL to directly hit in order to keep on consuming from the same REST Proxy.
- If the REST Proxy shuts down, it will try to gracefully close the consumers.
- We can set `auto.offset.reset` (latest or earliest).
- We can set `auto.commit.enable` (true or false).

### 4.1.2. Consuming with the REST Proxy - Binary

- **Steps are (for any data format)**
  1. Create consumer.
  2. Subscribe to a topic (or topic list).
  3. Get records.
  4. Process records (once in our app).
  5. Commit offsets (once in a while).
- Binary data will be read in base64.

## 4.2. Producing with the REST Proxy - JSON

- JSON data does not need to be transformed before being sent to the REST Proxy, as our POST request takes JSON as a payload.
- Any kind of valid JSON can be sent, there are no restrictions!
- Each language has support for JSON so that's a very easy way to send semi-structured data.
- It is the same as before, except the **header changes**.

### 4.2.1. Consuming with the REST Proxy - JSON

- **Steps are (for any data format)**
  1. Create consumer.
  2. Subscribe to a topic (or topic list).
  3. Get records.
  4. Process records (once in our app).
  5. Commit offsets (once in a while).
- JSON data will get read as JSON.
- The steps are the exact same as before, only the output and **header changes**.

## 4.3. Producing with the REST Proxy - Avro

- The REST Proxy has primary support for Avro as it's directly connected to the Schema Registry.
- We send the schema in JSON (stringified), and we send the Avro payload encoded in JSON.
- After the first produce call, we can get a schema id to re-use in the subsequent requests to make them smaller.

### 4.3.1. Consuming with the REST Proxy - Avro

- **Steps are (for any data format)**
  1. Create consumer.
  2. Subscribe to a topic (or topic list).
  3. Get records.
  4. Process records (once in our app).
  5. Commit offsets (once in a while).
- Avro data will get read as JSON encoded (like with avro-tools).
- The steps are the exact same as before, only the output and **header changes**.
