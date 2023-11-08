---
title: Pulsar
weight: 9
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Pulsar Connector

Flink provides an [Apache Pulsar](https://pulsar.apache.org) connector for reading and writing data from and to Pulsar topics with exactly-once guarantees.

## Dependency

You can use the connector with the Pulsar 3.0.0 or higher. It is recommended to always use the latest Pulsar version.
The details on Pulsar compatibility can be found in [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification).

{{< connector_artifact flink-connector-pulsar pulsar >}}

{{< py_connector_download_link "pulsar" >}}

Flink's streaming connectors are not part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

## Pulsar Source

{{< hint info >}}
This part describes the Pulsar source based on the new
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.
{{< /hint >}}

### Usage

The Pulsar source provides a builder class for constructing a PulsarSource instance. The code snippet below builds a PulsarSource instance. It consumes messages from the earliest cursor of the topic
"persistent://public/default/my-topic" in **Exclusive** subscription type (`my-subscription`)
and deserializes the raw payload of the messages as strings.

{{< tabs "pulsar-source-usage" >}}
{{< tab "Java" >}}

```java
PulsarSource<String> source = PulsarSource.builder()
    .setServiceUrl(serviceUrl)
    .setStartCursor(StartCursor.earliest())
    .setTopics("my-topic")
    .setDeserializationSchema(new SimpleStringSchema())
    .setSubscriptionName("my-subscription")
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
```

{{< /tab >}}
{{< tab "Python" >}}

```python
pulsar_source = PulsarSource.builder() \
    .set_service_url('pulsar://localhost:6650') \
    .set_admin_url('http://localhost:8080') \
    .set_start_cursor(StartCursor.earliest()) \
    .set_topics("my-topic") \
    .set_deserialization_schema(SimpleStringSchema()) \
    .set_subscription_name('my-subscription') \
    .build()

env.from_source(source=pulsar_source,
                watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                source_name="pulsar source")
```

{{< /tab >}}
{{< /tabs >}}

The following properties are **required** for building a PulsarSource:

- Pulsar service URL, configured by `setServiceUrl(String)`
- Pulsar service HTTP URL (also known as admin URL), configured by `setAdminUrl(String)`
- Pulsar subscription name, configured by `setSubscriptionName(String)`
- Topics / partitions to subscribe, see the following
  [topic-partition subscription](#topic-partition-subscription) for more details.
- Deserializer to parse Pulsar messages, see the following
  [deserializer](#deserializer) for more details.

It is recommended to set the consumer name in Pulsar Source by `setConsumerName(String)`.
This sets a unique name for the Flink connector in the Pulsar statistic dashboard.
You can use it to monitor the performance of your Flink connector and applications.

### Topic-partition Subscription

Pulsar source provide two ways of topic-partition subscription:

- Topic list, subscribing messages from all partitions in a list of topics. For example:
  {{< tabs "pulsar-source-topics" >}}
  {{< tab "Java" >}}

  ```java
  PulsarSource.builder().setTopics("some-topic1", "some-topic2");

  // Partition 0 and 2 of topic "topic-a"
  PulsarSource.builder().setTopics("topic-a-partition-0", "topic-a-partition-2");
  ```

  {{< /tab >}}
  {{< tab "Python" >}}

  ```python
  PulsarSource.builder().set_topics(["some-topic1", "some-topic2"])

  # Partition 0 and 2 of topic "topic-a"
  PulsarSource.builder().set_topics(["topic-a-partition-0", "topic-a-partition-2"])
  ```

  {{< /tab >}}
  {{< /tabs >}}

- Topic pattern, subscribing messages from all topics whose name matches the provided regular expression. For example:
  {{< tabs "pulsar-source-topic-pattern" >}}
  {{< tab "Java" >}}

  ```java
  PulsarSource.builder().setTopicPattern("topic-.*");
  ```

  {{< /tab >}}
  {{< tab "Python" >}}

  ```python
  PulsarSource.builder().set_topic_pattern("topic-.*")
  ```

  {{< /tab >}}
  {{< /tabs >}}

#### Flexible Topic Naming

Since Pulsar 2.0, all topic names internally are in a form of `{persistent|non-persistent}://tenant/namespace/topic`.
Now, for partitioned topics, you can use short names in many cases (for the sake of simplicity).
The flexible naming system stems from the fact that there is now a default topic type, tenant, and namespace in a Pulsar cluster.

| Topic property | Default      |
|:---------------|:-------------|
| topic type     | `persistent` |
| tenant         | `public`     |
| namespace      | `default`    |

This table lists a mapping relationship between your input topic name and the translated topic name:

| Input topic name                  | Translated topic name                          |
|:----------------------------------|:-----------------------------------------------|
| `my-topic`                        | `persistent://public/default/my-topic`         |
| `my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic` |

{{< hint warning >}}
For non-persistent topics, you need to specify the entire topic name,
as the default-based rules do not apply for non-partitioned topics.
Thus, you cannot use a short name like `non-persistent://my-topic` and need to use `non-persistent://public/default/my-topic` instead.
{{< /hint >}}

#### Subscribing Pulsar Topic Partition

Internally, Pulsar divides a partitioned topic as a set of non-partitioned topics according to the partition size.

For example, if a `simple-string` topic with 3 partitions is created under the `sample` tenant with the `flink` namespace.
The topics on Pulsar would be:

| Topic name                                            | Partitioned |
|:------------------------------------------------------|:------------|
| `persistent://sample/flink/simple-string`             | Y           |
| `persistent://sample/flink/simple-string-partition-0` | N           |
| `persistent://sample/flink/simple-string-partition-1` | N           |
| `persistent://sample/flink/simple-string-partition-2` | N           |

You can directly consume messages from the topic partitions by using the non-partitioned topic names above.
For example, use `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")`
would consume the partitions 1 and 2 of the `sample/flink/simple-string` topic.

#### Setting Topic Patterns

The Pulsar source can subscribe to a set of topics under only one tenant and one namespace by using regular expression.
But the topic type (`persistent` or `non-persistent`) isn't determined by the regular expression.
Even if you use `PulsarSource.builder().setTopicPattern("non-persistent://public/default/my-topic.*")`, we will subscribe both
`persistent` and `non-persistent` topics which its name matches `public/default/my-topic.*`.

In order to subscribe only `non-persistent` topics. You need to set the `RegexSubscriptionMode` to `RegexSubscriptionMode.NonPersistentOnly`.
For example, `setTopicPattern("topic-.*", RegexSubscriptionMode.NonPersistentOnly)`.
And use `setTopicPattern("topic-.*", RegexSubscriptionMode.PersistentOnly)` will only subscribe to the `persistent` topics.

The regular expression should follow the [topic naming pattern](#flexible-topic-naming). Only the topic name part can be
a regular expression. For example, if you provide a simple topic regular expression like `some-topic-\d`,
we will filter all the topics under the `public` tenant with the `default` namespace.
And if the topic regular expression is `flink/sample/topic-.*`, we will filter all the topics under the `flink` tenant with the `sample` namespace.

{{< hint warning >}}
Currently, the latest released Pulsar 2.11.0 didn't return the `non-persistent` topics correctly.
You can't use regular expression for filtering `non-persistent` topics in Pulsar 2.11.0.

See this issue: https://github.com/apache/pulsar/issues/19316 for the detailed context of this bug.
{{< /hint >}}

### Deserializer

A deserializer (`PulsarDeserializationSchema`) is for decoding Pulsar messages from bytes.
You can configure the deserializer using `setDeserializationSchema(PulsarDeserializationSchema)`.
The `PulsarDeserializationSchema` defines how to deserialize a Pulsar `Message<byte[]>`.

If only the raw payload of a message (message data in bytes) is needed,
you can use the predefined `PulsarDeserializationSchema`. Pulsar connector provides three implementation methods.

- Decode the message by using Pulsar's [Schema](https://pulsar.apache.org/docs/2.11.x/schema-understand/).
  If using KeyValue type or Struct types, the pulsar `Schema` does not contain type class info. But it is
  still needed to construct `PulsarSchemaTypeInformation`. So we provide two more APIs to pass the type info.
  ```java
  // Primitive types
  PulsarSourceBuilder.setDeserializationSchema(Schema);

  // Struct types (JSON, Protobuf, Avro, etc.)
  PulsarSourceBuilder.setDeserializationSchema(Schema, Class);

  // KeyValue type
  PulsarSourceBuilder.setDeserializationSchema(Schema, Class, Class);
  ```
- Decode the message by using Flink's `DeserializationSchema`
  ```java
  PulsarSourceBuilder.setDeserializationSchema(DeserializationSchema);
  ```
- Decode the message by using Flink's `TypeInformation`
  ```java
  PulsarSourceBuilder.setDeserializationSchema(TypeInformation, ExecutionConfig);
  ```

Pulsar `Message<byte[]>` contains some [extra properties](https://pulsar.apache.org/docs/2.11.x/concepts-messaging/#messages),
such as message key, message publish time, message time, and application-defined key/value pairs etc.
These properties could be defined in the `Message<byte[]>` interface.

If you want to deserialize the Pulsar message by these properties, you need to implement `PulsarDeserializationSchema`.
Ensure that the `TypeInformation` from the `PulsarDeserializationSchema.getProducedType()` is correct.
Flink uses this `TypeInformation` to pass the messages to downstream operators.

#### Schema Evolution in Source

[Schema evolution][schema-evolution] can be enabled by users using Pulsar's `Schema` and
`PulsarSourceBuilder.enableSchemaEvolution()`. This means that any broker schema validation is in place.

```java
Schema<SomePojo> schema = Schema.AVRO(SomePojo.class);

PulsarSource<SomePojo> source = PulsarSource.builder()
    ...
    .setDeserializationSchema(schema, SomePojo.class)
    .enableSchemaEvolution()
    .build();
```

If you use Pulsar schema without enabling schema evolution, we will bypass the schema check. This may cause some
errors when you use a wrong schema to deserialize the messages.

#### Use Auto Consume Schema

Pulsar provides `Schema.AUTO_CONSUME()` for consuming message without a predefined schema. This is always used when
the topic has multiple schemas and may not be compatible with each other. Pulsar will auto decode the message into a
`GenericRecord` for the user.

But the `PulsarSourceBuilder.setDeserializationSchema(Schema)` method doesn't support the `Schema.AUTO_CONSUME()`.
Instead, we provide the `GenericRecordDeserializer` for deserializing the `GenericRecord`. You can implement this
interface and set it in the `PulsarSourceBuilder.setDeserializationSchema(GenericRecordDeserializer)`.

```java
GenericRecordDeserializer<SomePojo> deserializer = ...
PulsarSource<SomePojo> source = PulsarSource.builder()
    ...
    .setDeserializationSchema(deserializer)
    .build();
```

{{< hint warning >}}
Currently, auto consume schema only supports AVRO, JSON and Protobuf schemas.
{{< /hint >}}

### Define a RangeGenerator

Ensure that you have provided a `RangeGenerator` implementation if you want to consume a subset of keys on the Pulsar connector.
The `RangeGenerator` generates a set of key hash ranges so that a respective reader subtask only dispatches
messages where the hash of the message key is contained in the specified range.

Since the Pulsar didn't expose the key hash range method. We have to provide an `FixedKeysRangeGenerator` for end-user.
You can add the keys you want to consume, no need to calculate any hash ranges.
The key's hash isn't specified to only one key, so the consuming results may contain the messages with
different keys comparing the keys you have defined in this range generator.
Remember to use flink's `DataStream.filter()` method after the Pulsar source.

```java
FixedKeysRangeGenerator.builder()
    .supportNullKey()
    .key("someKey")
    .keys(Arrays.asList("key1", "key2"))
    .build()
```

### Starting Position

The Pulsar source is able to consume messages starting from different positions by setting the `setStartCursor(StartCursor)` option.
Built-in start cursors include:

- Start from the earliest available message in the topic.
  {{< tabs "pulsar-starting-position-earliest" >}}
  {{< tab "Java" >}}
  ```java
  StartCursor.earliest();
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StartCursor.earliest()
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Start from the latest available message in the topic.
  {{< tabs "pulsar-starting-position-latest" >}}
  {{< tab "Java" >}}
  ```java
  StartCursor.latest();
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StartCursor.latest()
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Start from a specified message between the earliest and the latest.
The Pulsar connector consumes from the latest available message if the message ID does not exist.

  The start message is included in consuming result.
  {{< tabs "pulsar-starting-position-from-message-id" >}}
  {{< tab "Java" >}}
  ```java
  StartCursor.fromMessageId(MessageId);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StartCursor.from_message_id(message_id)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Start from a specified message between the earliest and the latest.
The Pulsar connector consumes from the latest available message if the message ID doesn't exist.

  Include or exclude the start message by using the second boolean parameter.
  {{< tabs "pulsar-starting-position-from-message-id-bool" >}}
  {{< tab "Java" >}}
  ```java
  StartCursor.fromMessageId(MessageId, boolean);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StartCursor.from_message_id(message_id, boolean)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Start from the specified message publish time by `Message<byte[]>.getPublishTime()`.
This method is deprecated because the name is totally wrong which may cause confuse.
You can use `StartCursor.fromPublishTime(long)` instead.

  {{< tabs "pulsar-starting-position-message-time" >}}
  {{< tab "Java" >}}
  ```java
  StartCursor.fromMessageTime(long);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StartCursor.from_message_time(int)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Start from the specified message publish time by `Message<byte[]>.getPublishTime()`.
  {{< tabs "pulsar-starting-position-publish-time" >}}
  {{< tab "Java" >}}
  ```java
  StartCursor.fromPublishTime(long);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StartCursor.from_publish_time(int)
  ```
  {{< /tab >}}
  {{< /tabs >}}

The `StartCursor` is used when the corresponding subscription is not created in Pulsar by default.
The priority of the consumption start position is, checkpoint > existed subscription position > `StartCursor`.
Sometimes, the end user may want to force the start position by using `StartCursor`. You should enable the `pulsar.source.resetSubscriptionCursor`
option and start the pipeline without the saved checkpoint files.
It is important to note that the given consumption position in the checkpoint is always the highest priority.

{{< hint info >}}
Each Pulsar message belongs to an ordered sequence on its topic.
The sequence ID (`MessageId`) of the message is ordered in that sequence.
The `MessageId` contains some extra information (the ledger, entry, partition) about how the message is stored,
you can create a `MessageId` by using `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)`.
{{< /hint >}}

### Boundedness

The Pulsar source supports streaming and batch execution mode.
By default, the `PulsarSource` is configured for unbounded data.

For unbounded data the Pulsar source never stops until a Flink job is stopped or failed.
You can use the `setUnboundedStopCursor(StopCursor)` to set the Pulsar source to stop at a specific stop position.

You can use `setBoundedStopCursor(StopCursor)` to specify a stop position for bounded data.

Built-in stop cursors include:

- The Pulsar source never stops consuming messages.
  {{< tabs "pulsar-boundedness-never" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.never();
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.never()
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop at the latest available message when the Pulsar source starts consuming messages.
  {{< tabs "pulsar-boundedness-latest" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.latest();
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.latest()
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop when the connector meets a given message, or stop at a message which is produced after this given message.
  {{< tabs "pulsar-boundedness-at-message-id" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.atMessageId(MessageId);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.at_message_id(message_id)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop but include the given message in the consuming result.
  {{< tabs "pulsar-boundedness-after-message-id" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.afterMessageId(MessageId);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.after_message_id(message_id)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop at the specified event time by `Message<byte[]>.getEventTime()`. The message with the
given event time won't be included in the consuming result.
  {{< tabs "pulsar-boundedness-at-event-time" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.atEventTime(long);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.at_event_time(int)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop after the specified event time by `Message<byte[]>.getEventTime()`. The message with the
given event time will be included in the consuming result.
  {{< tabs "pulsar-boundedness-after-event-time" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.afterEventTime(long);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.after_event_time(int)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop at the specified publish time by `Message<byte[]>.getPublishTime()`. The message with the
given publish time won't be included in the consuming result.
  {{< tabs "pulsar-boundedness-at-publish-time" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.atPublishTime(long);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.at_publish_time(int)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- Stop after the specified publish time by `Message<byte[]>.getPublishTime()`. The message with the
given publish time will be included in the consuming result.
  {{< tabs "pulsar-boundedness-after-publish-time" >}}
  {{< tab "Java" >}}
  ```java
  StopCursor.afterPublishTime(long);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  StopCursor.after_publish_time(int)
  ```
  {{< /tab >}}
  {{< /tabs >}}

### Source Configurable Options

In addition to configuration options described above, you can set arbitrary options for `PulsarClient`,
`PulsarAdmin`, Pulsar `Consumer` and `PulsarSource` by using `setConfig(ConfigOption<T>, T)`,
`setConfig(Configuration)` and `setConfig(Properties)`.

#### PulsarClient Options

The Pulsar connector uses the [client API](https://pulsar.apache.org/docs/2.11.x/client-libraries-java/)
to create the `Consumer` instance. The Pulsar connector extracts most parts of Pulsar's `ClientConfigurationData`,
which is required for creating a `PulsarClient`, as Flink configuration options in `PulsarOptions`.

{{< generated/pulsar_client_configuration >}}

#### PulsarAdmin Options

The [admin API](https://pulsar.apache.org/docs/2.11.x/admin-api-overview/) is used for querying topic metadata
and for discovering the desired topics when the Pulsar connector uses topic-pattern subscription.
It shares most part of the configuration options with the client API.
The configuration options listed here are only used in the admin API.
They are also defined in `PulsarOptions`.

{{< generated/pulsar_admin_configuration >}}

#### Pulsar Consumer Options

In general, Pulsar provides the Reader API and Consumer API for consuming messages in different scenarios.
The Pulsar connector uses the Consumer API. It extracts most parts of Pulsar's `ConsumerConfigurationData` as Flink configuration options in `PulsarSourceOptions`.

{{< generated/pulsar_consumer_configuration >}}

#### PulsarSource Options

The configuration options below are mainly used for customizing the performance and message acknowledgement behavior.
You can ignore them if you do not have any performance issues.

{{< generated/pulsar_source_configuration >}}

### Dynamic Partition Discovery

To handle scenarios like topic scaling-out or topic creation without restarting the Flink
job, the Pulsar source periodically discover new partitions under a provided
topic-partition subscription pattern. To enable partition discovery, you can set a non-negative value for
the `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS` option:

{{< tabs "pulsar-dynamic-partition-discovery" >}}
{{< tab "Java" >}}

```java
// discover new partitions per 10 seconds
PulsarSource.builder()
    .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```

{{< /tab >}}
{{< tab "Python" >}}

```python
# discover new partitions per 10 seconds
PulsarSource.builder()
    .set_config("pulsar.source.partitionDiscoveryIntervalMs", 10000)
```

{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
- Partition discovery is **enabled** by default. The Pulsar connector queries the topic metadata every 5 minutes.
- To disable partition discovery, you need to set a negative partition discovery interval.
- Partition discovery is disabled for bounded data even if you set this option with a non-negative value.
{{< /hint >}}

### Event Time and Watermarks

By default, the message uses the timestamp embedded in Pulsar `Message<byte[]>` as the event time.
You can define your own `WatermarkStrategy` to extract the event time from the message,
and emit the watermark downstream:

{{< tabs "pulsar-watermarks" >}}
{{< tab "Java" >}}

```java
env.fromSource(pulsarSource, new CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy");
```

{{< /tab >}}
{{< tab "Python" >}}

```python
env.from_source(pulsar_source, CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy")
```

{{< /tab >}}
{{< /tabs >}}

[This documentation]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}) describes
details about how to define a `WatermarkStrategy`.

### Message Acknowledgement

When a subscription is created, Pulsar [retains](https://pulsar.apache.org/docs/2.11.x/concepts-architecture-overview/#persistent-storage) all messages,
even if the consumer is disconnected. The retained messages are discarded only when the connector acknowledges that all these messages are processed successfully.

We use `Exclusive` subscription as the default subscription type. It supports cumulative acknowledgment. In this subscription type,
Flink only needs to acknowledge the latest successfully consumed message. All the message before the given message are marked
with a consumed status.

The Pulsar source acknowledges the current consuming message when checkpoints are **completed**,
to ensure the consistency between Flink's checkpoint state and committed position on the Pulsar brokers.

If checkpointing is disabled, Pulsar source periodically acknowledges messages.
You can use the `PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL` option to set the acknowledgement period.

Pulsar source does **NOT** rely on committed positions for fault tolerance.
Acknowledging messages is only for exposing the progress of consumers and monitoring on these two subscription types.

## Pulsar Sink

The Pulsar Sink supports writing records into one or more Pulsar topics or a specified list of Pulsar partitions.

{{< hint info >}}
This part describes the Pulsar sink based on the new
[data sink](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) API.

If you still want to use the legacy `SinkFunction` or on Flink 1.14 or previous releases, just use the StreamNative's
[pulsar-flink](https://github.com/streamnative/pulsar-flink).
{{< /hint >}}

### Usage

The Pulsar Sink uses a builder class to construct the `PulsarSink` instance.
This example writes a String record to a Pulsar topic with at-least-once delivery guarantee.

{{< tabs "pulsar-sink-example" >}}
{{< tab "Java" >}}

```java
DataStream<String> stream = ...

PulsarSink<String> sink = PulsarSink.builder()
    .setServiceUrl(serviceUrl)
    .setAdminUrl(adminUrl)
    .setTopics("topic1")
    .setSerializationSchema(new SimpleStringSchema())
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();

stream.sinkTo(sink);
```

{{< /tab >}}
{{< tab "Python" >}}

```python
stream = ...

pulsar_sink = PulsarSink.builder() \
    .set_service_url('pulsar://localhost:6650') \
    .set_admin_url('http://localhost:8080') \
    .set_topics("topic1") \
    .set_serialization_schema(SimpleStringSchema()) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

stream.sink_to(pulsar_sink)
```

{{< /tab >}}
{{< /tabs >}}

The following properties are **required** for building PulsarSink:

- Pulsar service url, configured by `setServiceUrl(String)`
- Pulsar service http url (aka. admin url), configured by `setAdminUrl(String)`
- Topics / partitions to write, see [Producing to topics](#producing-to-topics) for more details.
- Serializer to generate Pulsar messages, see [serializer](#serializer) for more details.

It is recommended to set the producer name in Pulsar Source by `setProducerName(String)`.
This sets a unique name for the Flink connector in the Pulsar statistic dashboard.
You can use it to monitor the performance of your Flink connector and applications.

### Producing to topics

Defining the topics for producing is similar to the [topic-partition subscription](#topic-partition-subscription)
in the Pulsar source. We support a mix-in style of topic setting. You can provide a list of topics,
partitions, or both of them.

{{< tabs "set-pulsar-sink-topics" >}}
{{< tab "Java" >}}

```java
// Topic "some-topic1" and "some-topic2"
PulsarSink.builder().setTopics("some-topic1", "some-topic2")

// Partition 0 and 2 of topic "topic-a"
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")

// Partition 0 and 2 of topic "topic-a" and topic "some-topic2"
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2", "some-topic2")
```

{{< /tab >}}
{{< tab "Python" >}}

```python
# Topic "some-topic1" and "some-topic2"
PulsarSink.builder().set_topics(["some-topic1", "some-topic2"])

# Partition 0 and 2 of topic "topic-a"
PulsarSink.builder().set_topics(["topic-a-partition-0", "topic-a-partition-2"])

# Partition 0 and 2 of topic "topic-a" and topic "some-topic2"
PulsarSink.builder().set_topics(["topic-a-partition-0", "topic-a-partition-2", "some-topic2"])
```

{{< /tab >}}
{{< /tabs >}}

The topics you provide support auto partition discovery. We query the topic metadata from the Pulsar in a fixed interval.
You can use the `PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` option to change the discovery interval option.

Configuring writing targets can be replaced by using a custom [`TopicRouter`]
[message routing](#message-routing). Configuring partitions on the Pulsar connector is explained in the [flexible topic naming](#flexible-topic-naming) section.

{{< hint warning >}}
If you build the Pulsar sink based on both the topic and its corresponding partitions, Pulsar sink merges them and only uses the topic.

For example, when using the `PulsarSink.builder().setTopics("some-topic1", "some-topic1-partition-0")` option to build the Pulsar sink,
this is simplified to `PulsarSink.builder().setTopics("some-topic1")`.
{{< /hint >}}

#### Dynamic Topics by incoming messages

Topics could be defined by the incoming messages instead of providing the fixed topic set in builder. You can dynamically
provide the topic by in a custom `TopicRouter`. The topic metadata can be queried by using `PulsarSinkContext.topicMetadata(String)`
and the query result would be cached and expire in `PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` milliseconds.

If you want to write to a non-existed topic, just return it in `TopicRouter`. Pulsar connector will **try to create it**.

{{< hint warning >}}
You need to enable the topic auto creation in Pulsar's `broker.conf` when you want to write messages to a non-existed topic.
Set the `allowAutoTopicCreation=true` to enable it.

The `allowAutoTopicCreationType` option in `broker.conf` is used to control the type of topic that is allowed to be automatically created.

- `non-partitioned`: The default type for the auto-created topic.
  It doesn't have any partition and can't be converted to a partitioned topic.
- `partitioned`: The topic will be created as a partitioned topic.
  Set the `defaultNumPartitions` option to control the auto created partition size.
{{< /hint >}}

### Serializer

A serializer (`PulsarSerializationSchema`) is required for serializing the record instance into bytes.
Similar to `PulsarSource`, Pulsar sink supports both Flink's `SerializationSchema` and
Pulsar's `Schema`. But Pulsar's `Schema.AUTO_PRODUCE_BYTES()` is not supported.

If you do not need the message key and other message properties in Pulsar's
[Message](https://pulsar.apache.org/api/client/2.10.x/org/apache/pulsar/client/api/Message.html) interface,
you can use the predefined `PulsarSerializationSchema`. The Pulsar sink provides two implementation methods.

- Encode the message by using Pulsar's [Schema](https://pulsar.apache.org/docs/2.11.x/schema-understand/).
  ```java
  // Primitive types
  PulsarSinkBuilder.setSerializationSchema(Schema)

  // Struct types (JSON, Protobuf, Avro, etc.)
  PulsarSinkBuilder.setSerializationSchema(Schema, Class)

  // KeyValue type
  PulsarSinkBuilder.setSerializationSchema(Schema, Class, Class)
  ```
- Encode the message by using Flink's `SerializationSchema`
  ```java
  PulsarSinkBuilder.setSerializationSchema(SerializationSchema)
  ```

#### Schema Evolution in Sink

[Schema evolution][schema-evolution] can be enabled by users using Pulsar's `Schema`
and `PulsarSinkBuilder.enableSchemaEvolution()`. This means that any broker schema validation is in place.

```java
Schema<SomePojo> schema = Schema.AVRO(SomePojo.class);

PulsarSink<SomePojo> sink = PulsarSink.builder()
    ...
    .setSerializationSchema(schema, SomePojo.class)
    .enableSchemaEvolution()
    .build();
```

{{< hint warning >}}
If you use Pulsar schema without enabling schema evolution, the target topic will have a `Schema.BYTES` schema.
But `Schema.BYTES` isn't stored in any Pulsar's topic. An auto-created topic in this way will present no schema.
Consumers will need to handle deserialization without Pulsar's `Schema` (if needed) themselves.

For example, if you set `PulsarSinkBuilder.setSerializationSchema(Schema.STRING)` without enabling schema evolution,
the schema stored in Pulsar topics is `Schema.BYTES`.
{{< /hint >}}

#### PulsarMessage<byte[]> validation

Pulsar topic always has at least one schema. The `Schema.BYTES` is the default one for any topic without schema being set.
But sending messages bytes with `Schema.BYTES` bypass the schema validate. So the message sent with `SerializationSchema`
and `Schema` which doesn't enable the schema evolution may save the invalid messages in the topic.

You can enable the `pulsar.sink.validateSinkMessageBytes` option to let the connector use the Pulsar's `Schema.AUTO_PRODUCE_BYTES()`
which supports extra check for the message bytes before sending. It will query the latest schema in topic and use it to
validate the message bytes.

But some schemas in Pulsar don't support validation, so we disable this option by default. you should use it at your own risk.

#### Custom serializer

You can have your own serialization logic by implementing the `PulsarSerializationSchema` interface. The return type
for this interface is `PulsarMessage` which you can't create it directly. Instead, we use builder method for creating
three types of the Pulsar messages.

- Create a message with a Pulsar `Scheme`. This is always when you know the schema in topic.
  We will check if the given schema is compatible in the correspond topic.
  ```java
  PulsarMessage.builder(Schema<M> schema, M message)
      ...
      .build();
  ```
- Create the message without any Pulsar `Scheme`. The message type can only be the byte array.
  It won't validate the message bytes by default.
  ```java
  PulsarMessage.builder(byte[] bytes)
      ...
      .build();
  ```
- Create a tombstone message with empty payloads. [Tombstone][tombstone-data-store] is a special message which is
  supported in Pulsar.
  ```java
  PulsarMessage.builder()
      ...
      .build();
  ```

### Message Routing

Routing in Pulsar Sink is operated on the partition level. For a list of partitioned topics,
the routing algorithm first collects all partitions from different topics, and then calculates routing within all the partitions.
By default, Pulsar Sink supports two router implementations.

- `KeyHashTopicRouter`: use the hashcode of the message's key to decide the topic partition that messages are sent to.

  The message key is provided by `PulsarSerializationSchema.key(IN, PulsarSinkContext)`
  You need to implement this interface and extract the message key when you want to send the message with the same key to the same topic partition.

  If you do not provide the message key. A topic  partition is randomly chosen from the topic list.

  The message key can be hashed in two ways: `MessageKeyHash.JAVA_HASH` and `MessageKeyHash.MURMUR3_32_HASH`.
  You can use the `PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH` option to choose the hash method.

- `RoundRobinRouter`: Round-robin among all the partitions.

  All messages are sent to the first partition, and switch to the next partition after sending
  a fixed number of messages. The batch size can be customized by the `PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES` option.

Let’s assume there are ten messages and two topics. Topic A has two partitions while topic B has three partitions.
The batch size is set to five messages. In this case, topic A has 5 messages per partition which topic B does not receive any messages.

You can configure custom routers by using the `TopicRouter` interface.
If you implement a `TopicRouter`, ensure that it is serializable.
And you can return partitions which are not available in the pre-discovered partition list.

Thus, you do not need to specify topics using the `PulsarSinkBuilder.setTopics` option when you implement the custom topic router.

```java
@PublicEvolving
public interface TopicRouter<IN> extends Serializable {

    TopicPartition route(IN in, List<TopicPartition> partitions, PulsarSinkContext context);

    default void open(SinkConfiguration sinkConfiguration) {
        // Nothing to do by default.
    }
}
```

{{< hint info >}}
Internally, a Pulsar partition is implemented as a topic. The Pulsar client provides APIs to hide this
implementation detail and handles routing under the hood automatically. Pulsar Sink uses a lower client
API to implement its own routing layer to support multiple topics routing.

For details, see [partitioned topics](https://pulsar.apache.org/docs/2.11.x/cookbooks-partitioned/).
{{< /hint >}}

### Delivery Guarantee

`PulsarSink` supports three delivery guarantee semantics.

- `NONE`: Data loss can happen even when the pipeline is running.
  Basically, we use a fire-and-forget strategy to send records to Pulsar topics in this mode.
  It means that this mode has the highest throughput.
- `AT_LEAST_ONCE`: No data loss happens, but data duplication can happen after a restart from checkpoint.
- `EXACTLY_ONCE`: No data loss happens. Each record is sent to the Pulsar broker only once.
  Pulsar Sink uses [Pulsar transaction](https://pulsar.apache.org/docs/2.11.x/transactions/)
  and two-phase commit (2PC) to ensure records are sent only once even after the pipeline restarts.

{{< hint warning >}}
If you want to use `EXACTLY_ONCE`, make sure you have enabled the checkpoint on Flink and enabled the transaction on Pulsar.
The Pulsar sink will write all the messages in a pending transaction and commit it after the successfully checkpointing.

The messages written to Pulsar after a pending transaction won't be obtained based on the design of the Pulsar.
You can acquire these messages only when the corresponding transaction is committed.
{{< /hint >}}

### Delayed message delivery

[Delayed message delivery](https://pulsar.apache.org/docs/2.11.x/concepts-messaging/#delayed-message-delivery)
enables you to delay the possibility to consume a message. With delayed message enabled, the Pulsar sink sends a message to the Pulsar topic
**immediately**, but the message is delivered to a consumer once the specified delay is over.

Delayed message delivery only works in the `Shared` subscription type. In `Exclusive` and `Failover`
subscription types, the delayed message is dispatched immediately.

You can configure the `MessageDelayer` to define when to send the message to the consumer.
The default option is to never delay the message dispatching. You can use the `MessageDelayer.fixed(Duration)` option to
Configure delaying all messages in a fixed duration. You can also implement the `MessageDelayer`
interface to dispatch messages at different time.

{{< hint warning >}}
The dispatch time should be calculated by the `PulsarSinkContext.processTime()`.
{{< /hint >}}

### Sink Configurable Options

You can set options for `PulsarClient`, `PulsarAdmin`, Pulsar `Producer` and `PulsarSink`
by using `setConfig(ConfigOption<T>, T)`, `setConfig(Configuration)` and `setConfig(Properties)`.

#### PulsarClient and PulsarAdmin Options

For details, refer to [PulsarAdmin options](#pulsaradmin-options).

#### Pulsar Producer Options

The Pulsar connector uses the Producer API to send messages. It extracts most parts of
Pulsar's `ProducerConfigurationData` as Flink configuration options in `PulsarSinkOptions`.

{{< generated/pulsar_producer_configuration >}}

#### PulsarSink Options

The configuration options below are mainly used for customizing the performance and message
sending behavior. You can just leave them alone if you do not have any performance issues.

{{< generated/pulsar_sink_configuration >}}

### Brief Design Rationale

Pulsar sink follow the Sink API defined in
[FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction).

#### Stateless SinkWriter

In `EXACTLY_ONCE` mode, the Pulsar sink does not store transaction information in a checkpoint.
That means that new transactions will be created after a restart.
Therefore, any message in previous pending transactions is either aborted or timed out
(They are never visible to the downstream Pulsar consumer).
The Pulsar team is working to optimize the needed resources by unfinished pending transactions.

#### Pulsar Schema Evolution

[Pulsar Schema Evolution][schema-evolution] allows
you to reuse the same Flink job after certain "allowed" data model changes, like adding or deleting
a field in a AVRO-based Pojo class. Please note that you can specify Pulsar schema validation rules
and define an auto schema update. For details, refer to [Pulsar Schema Evolution][schema-evolution].

## Monitor the Metrics

The Pulsar client refreshes its stats every 60 seconds by default. To increase the metrics refresh frequency,
you can change the Pulsar client stats refresh interval to a smaller value (minimum 1 second), as shown below.

{{< tabs "pulsar-stats-interval-seconds" >}}

{{< tab "Java" >}}
```java
builder.setConfig(PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS, 1L);
```
{{< /tab >}}

{{< tab "Python" >}}
```python
builder.set_config("pulsar.client.statsIntervalSeconds", "1")
```
{{< /tab >}}

{{< /tabs >}}

### Source Metrics

Flink defines common source metrics in [FLIP-33: Standardize Connector Metrics][standard-metrics]. Pulsar connector will
expose some client metrics if you enable the `pulsar.source.enableMetrics` option. All the custom source metrics are
listed in below table.

{{< tabs "pulsar-enable-source-metrics" >}}

{{< tab "Java" >}}
```java
builder.setConfig(PulsarSourceOptions.PULSAR_ENABLE_SOURCE_METRICS, true);
```
{{< /tab >}}

{{< tab "Python" >}}
```python
builder.set_config("pulsar.source.enableMetrics", "true")
```
{{< /tab >}}

{{< /tabs >}}

| Metrics                                                        | User Variables      | Description                                                        | Type  |
|----------------------------------------------------------------|---------------------|--------------------------------------------------------------------|-------|
| PulsarConsumer."Topic"."ConsumerName".numMsgsReceived          | Topic, ConsumerName | Number of messages received in the last interval.                  | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numBytesReceived         | Topic, ConsumerName | Number of bytes received in the last interval.                     | Gauge |
| PulsarConsumer."Topic"."ConsumerName".rateMsgsReceived         | Topic, ConsumerName | Rate of bytes per second received in the last interval.            | Gauge |
| PulsarConsumer."Topic"."ConsumerName".rateBytesReceived        | Topic, ConsumerName | Rate of bytes per second received in the last interval.            | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numAcksSent              | Topic, ConsumerName | Number of message acknowledgments sent in the last interval.       | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numAcksFailed            | Topic, ConsumerName | Number of message acknowledgments failed in the last interval.     | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numReceiveFailed         | Topic, ConsumerName | Number of message receive failed in the last interval.             | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numBatchReceiveFailed    | Topic, ConsumerName | Number of message batch receive failed in the last interval.       | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalMsgsReceived        | Topic, ConsumerName | Total number of messages received by this consumer.                | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalBytesReceived       | Topic, ConsumerName | Total number of bytes received by this consumer.                   | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalReceivedFailed      | Topic, ConsumerName | Total number of messages receive failures by this consumer.        | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalBatchReceivedFailed | Topic, ConsumerName | Total number of messages batch receive failures by this consumer.  | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalAcksSent            | Topic, ConsumerName | Total number of message acknowledgments sent by this consumer.     | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalAcksFailed          | Topic, ConsumerName | Total number of message acknowledgments failures on this consumer. | Gauge |
| PulsarConsumer."Topic"."ConsumerName".msgNumInReceiverQueue    | Topic, ConsumerName | The size of receiver queue on this consumer.                       | Gauge |

### Sink Metrics

The below table lists supported sink metrics. The first 6 metrics are standard Pulsar Sink metrics as described in
[FLIP-33: Standardize Connector Metrics][standard-metrics].

The first 5 metrics are exposed to the flink metric system by default.
You should enable the `pulsar.sink.enableMetrics` option to get the remaining metrics exposed.

{{< tabs "pulsar-enable-sink-metrics" >}}

{{< tab "Java" >}}
```java
builder.setConfig(PulsarSinkOptions.PULSAR_ENABLE_SINK_METRICS, true);
```
{{< /tab >}}

{{< tab "Python" >}}
```python
builder.set_config("pulsar.sink.enableMetrics", "true")
```
{{< /tab >}}

{{< /tabs >}}

| Metrics                                                       | User Variables      | Description                                                                                                  | Type    |
|---------------------------------------------------------------|---------------------|--------------------------------------------------------------------------------------------------------------|---------|
| numBytesOut                                                   | n/a                 | The total number of output bytes since the sink starts. Count towards the numBytesOut in TaskIOMetricsGroup. | Counter |
| numBytesOutPerSecond                                          | n/a                 | The output bytes per second.                                                                                 | Meter   |
| numRecordsOut                                                 | n/a                 | The total number of output records since the sink starts.                                                    | Counter |
| numRecordsOutPerSecond                                        | n/a                 | The output records per second.                                                                               | Meter   |
| numRecordsOutErrors                                           | n/a                 | The total number of records failed to send.                                                                  | Counter |
| currentSendTime                                               | n/a                 | The time it takes to send the last record, from enqueue the message in client buffer to its ack.             | Gauge   |
| PulsarProducer."Topic"."ProducerName".numMsgsSent             | Topic, ProducerName | The number of messages published in the last interval.                                                       | Gauge   |
| PulsarProducer."Topic"."ProducerName".numBytesSent            | Topic, ProducerName | The number of bytes sent in the last interval.                                                               | Gauge   |
| PulsarProducer."Topic"."ProducerName".numSendFailed           | Topic, ProducerName | The number of failed send operations in the last interval.                                                   | Gauge   |
| PulsarProducer."Topic"."ProducerName".numAcksReceived         | Topic, ProducerName | The number of send acknowledges received by broker in the last interval.                                     | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendMsgsRate            | Topic, ProducerName | The messages send rate in the last interval.                                                                 | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendBytesRate           | Topic, ProducerName | The bytes send rate in the last interval.                                                                    | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis50pct  | Topic, ProducerName | The 50% of send latency in milliseconds for the last interval.                                               | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis75pct  | Topic, ProducerName | The 75% of send latency in milliseconds for the last interval.                                               | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis95pct  | Topic, ProducerName | The 95% of send latency in milliseconds for the last interval.                                               | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis99pct  | Topic, ProducerName | The 99% of send latency in milliseconds for the last interval.                                               | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis999pct | Topic, ProducerName | The 99.9% of send latency in milliseconds for the last interval.                                             | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillisMax    | Topic, ProducerName | The maximum send latency in milliseconds for the last interval.                                              | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalMsgsSent           | Topic, ProducerName | The total number of messages published by this producer.                                                     | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalBytesSent          | Topic, ProducerName | The total number of bytes sent by this producer.                                                             | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalSendFailed         | Topic, ProducerName | The total number of failed send operations.                                                                  | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalAcksReceived       | Topic, ProducerName | The total number of send acknowledges received by broker.                                                    | Gauge   |
| PulsarProducer."Topic"."ProducerName".pendingQueueSize        | Topic, ProducerName | The current pending send-message queue size of the producer.                                                 | Gauge   |

{{< hint info >}}
- `numBytesOut`, `numRecordsOut` and `numRecordsOutErrors` are retrieved from Pulsar client metrics.

- `numBytesOutPerSecond` and `numRecordsOutPerSecond` are calculated based on the `numBytesOut` and `numRecordsOUt`
  counter respectively. Flink internally uses a fixed 60-seconds window to calculate the rates.

- `currentSendTime` tracks the time from when the producer calls `sendAync()` to
  the time when the broker acknowledges the message. This metric is not available in `NONE` delivery guarantee.
{{< /hint >}}

## End-to-end encryption

Flink can use Pulsar's encryption to encrypt messages on the sink side and decrypt messages on the source side.
Users should provide the public and private key pair to perform the encryption.
Only with a valid key pair can decrypt the encrypted messages.

### How to enable end-to-end encryption

1. Generate a set of key pairs.

   Pulsar supports multiple ECDSA or RSA key pairs in the meantime, you can provide
   multiple key pairs. We will randomly choose a key pair to encrypt the message which makes the encryption more secure.
   ```shell
   # ECDSA (for Java clients only)
   openssl ecparam -name secp521r1 -genkey -param_enc explicit -out test_ecdsa_privkey.pem
   openssl ec -in test_ecdsa_privkey.pem -pubout -outform pem -out test_ecdsa_pubkey.pem

   # RSA
   openssl genrsa -out test_rsa_privkey.pem 2048
   openssl rsa -in test_rsa_privkey.pem -pubout -outform pkcs8 -out test_rsa_pubkey.pem
   ```

2. Implement the `CryptoKeyReader` interface.

   Each key pair should have a unique key name. Implement the `CryptoKeyReader` interface and make sure
   `CryptoKeyReader.getPublicKey()` and `CryptoKeyReader.getPrivateKey()` can return the corresponding key by the
   key name.

   Pulsar provided a default `CryptoKeyReader` implementation named `DefaultCryptoKeyReader`. You can create it by using
   the `DefaultCryptoKeyReader.builder()`. And make sure the key pair files should be placed on the Flink running environment.

   ```java
   // defaultPublicKey and defaultPrivateKey should be provided in this implementation.
   // The file:///path/to/default-public.key should be a valid path on Flink's running environment.
   CryptoKeyReader keyReader = DefaultCryptoKeyReader.builder()
       .defaultPublicKey("file:///path/to/default-public.key")
       .defaultPrivateKey("file:///path/to/default-private.key")
       .publicKey("key1", "file:///path/to/public1.key").privateKey("key1", "file:///path/to/private1.key")
       .publicKey("key2", "file:///path/to/public2.key").privateKey("key2", "file:///path/to/private2.key")
       .build();
   ```

3. (Optional) Implement the `MessageCrypto<MessageMetadata, MessageMetadata>` interface.

   Pulsar supports the **ECDSA**, **RSA** out of box. You don't need to implement this interface if you use the common
   existing encryption methods. If you want to define a custom key pair based crypto method, just implement the
   `MessageCrypto<MessageMetadata, MessageMetadata>` interface. You can read the Pulsar's default implementation, the
   `MessageCryptoBc`, for how to implement this crypto interface.

4. Create `PulsarCrypto` instance.

   `PulsarCrypto` is used for providing all the required information for encryption and decryption. You can use the builder
   method to create the instance.

   ```java
   CryptoKeyReader keyReader = DefaultCryptoKeyReader.builder()
       .defaultPublicKey("file:///path/to/public1.key")
       .defaultPrivateKey("file:///path/to/private2.key")
       .publicKey("key1", "file:///path/to/public1.key").privateKey("key1", "file:///path/to/private1.key")
       .publicKey("key2", "file:///path/to/public2.key").privateKey("key2", "file:///path/to/private2.key")
       .build();

   // This line is only used as an example. It returns the default implementation of the MessageCrypto.
   SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>> cryptoSupplier = () -> new MessageCryptoBc();

   PulsarCrypto pulsarCrypto = PulsarCrypto.builder()
       .cryptoKeyReader(keyReader)
       // All the key name should be provided here, you can't encrypt the message with any non-existed key names.
       .addEncryptKeys("key1", "key2")
       // You don't have to provide the MessageCrypto.
       .messageCrypto(cryptoSupplier)
       .build()
   ```

### Decrypt the message on the Pulsar source

Follow the previous instruction to create a `PulsarCrypto` instance and pass it to the `PulsarSource.builder()`.
You need to choose the decrypt failure action in the meantime. Pulsar has three types of failure action which defines in
`ConsumerCryptoFailureAction`.

- `ConsumerCryptoFailureAction.FAIL`: The Flink pipeline will crash and turn into a failed state.
- `ConsumerCryptoFailureAction.DISCARD`: Message is silently drop and not delivered to the downstream.
- `ConsumerCryptoFailureAction.CONSUME`

  The message will not be decrypted and directly passed to downstream. You can decrypt the message in
  `PulsarDeserializationSchema`, the encryption information can be retrieved from `Message.getEncryptionCtx()`.

```java
PulsarCrypto pulsarCrypto = ...

PulsarSource<String> sink = PulsarSource.builder()
    ...
    .setPulsarCrypto(pulsarCrypto, ConsumerCryptoFailureAction.FAIL)
    .build();
```

### Encrypt the message on the Pulsar sink

Follow the previous instruction to create a `PulsarCrypto` instance and pass it to the `PulsarSink.builder()`.
You need to choose the encrypt failure action in the meantime. Pulsar has two types of failure action which defines in
`ProducerCryptoFailureAction`.

- `ProducerCryptoFailureAction.FAIL`: The Flink pipeline will crash and turn into a failed state.
- `ProducerCryptoFailureAction.SEND`: Send the unencrypted messages.

```java
PulsarCrypto pulsarCrypto = ...

PulsarSink<String> sink = PulsarSink.builder()
    ...
    .setPulsarCrypto(pulsarCrypto, ProducerCryptoFailureAction.FAIL)
    .build();
```

## Upgrading to the Latest Connector Version

The generic upgrade steps are outlined in [upgrading jobs and Flink versions guide]({{< ref "docs/ops/upgrading" >}}).
The Pulsar connector does not store any state on the Flink side. The Pulsar connector pushes and stores all the states on the Pulsar side.
For Pulsar, you additionally need to know these limitations:

- Do not upgrade the Pulsar connector and Pulsar broker version at the same time.
- Always use a newer Pulsar client with Pulsar connector to consume messages from Pulsar.

## Troubleshooting

If you have a problem with Pulsar when using Flink, keep in mind that Flink only wraps
[PulsarClient](https://pulsar.apache.org/api/client/2.10.x/) or
[PulsarAdmin](https://pulsar.apache.org/api/admin/2.10.x/)
and your problem might be independent of Flink and sometimes can be solved by upgrading Pulsar brokers,
reconfiguring Pulsar brokers or reconfiguring Pulsar connector in Flink.

{{< top >}}

[schema-evolution]: https://pulsar.apache.org/docs/2.11.x/schema-evolution-compatibility/#schema-evolution
[standard-metrics]: https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics
[tombstone-data-store]: https://en.wikipedia.org/wiki/Tombstone_%28data_store%29