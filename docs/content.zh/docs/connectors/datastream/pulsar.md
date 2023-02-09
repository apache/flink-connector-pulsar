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

# Apache Pulsar 连接器

Flink 当前提供 [Apache Pulsar](https://pulsar.apache.org) Source 和 Sink 连接器，用户可以使用它从 Pulsar 读取数据，并保证每条数据只被处理一次。

## 添加依赖

当前支持 Pulsar 2.10.0 及其之后的版本，建议在总是将 Pulsar 升级至最新版。如果想要了解更多关于 Pulsar API 兼容性设计，可以阅读文档 [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification)。

{{< artifact flink-connector-pulsar >}}

{{< py_download_link "pulsar" >}}

Flink 的流连接器并不会放到发行文件里面一同发布，阅读[此文档]({{< ref "docs/dev/configuration/overview" >}})，了解如何将连接器添加到集群实例内。

## Pulsar Source

{{< hint info >}}
Pulsar Source 基于 Flink 最新的[批流一体 API]({{< ref "docs/dev/datastream/sources.md" >}}) 进行开发。
{{< /hint >}}

### 使用示例

Pulsar Source 提供了 builder 类来构造 `PulsarSource` 实例。下面的代码实例使用 builder 类创建的实例会从 “persistent://public/default/my-topic” 的数据开始端进行消费。对应的 Pulsar Source 使用了 **Exclusive**（独占）的订阅方式消费消息，订阅名称为 `my-subscription`，并把消息体的二进制字节流以 UTF-8 的方式编码为字符串。

{{< tabs "pulsar-source-usage" >}}
{{< tab "Java" >}}

```java
PulsarSource<String> source = PulsarSource.builder()
    .setServiceUrl(serviceUrl)
    .setAdminUrl(adminUrl)
    .setStartCursor(StartCursor.earliest())
    .setTopics("my-topic")
    .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
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
    .set_deserialization_schema(
        PulsarDeserializationSchema.flink_schema(SimpleStringSchema())) \
    .set_subscription_name('my-subscription') \
    .build()

env.from_source(source=pulsar_source,
                watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                source_name="pulsar source")
```

{{< /tab >}}
{{< /tabs >}}

如果使用构造类构造 `PulsarSource`，一定要提供下面几个属性：

- Pulsar 数据消费的地址，使用 `setServiceUrl(String)` 方法提供。
- Pulsar HTTP 管理地址，使用 `setAdminUrl(String)` 方法提供。
- Pulsar 订阅名称，使用 `setSubscriptionName(String)` 方法提供。
- 需要消费的 Topic 或者是 Topic 下面的分区，详见[指定消费的 Topic 或者 Topic 分区](#指定消费的-topic-或者-topic-分区)。
- 解码 Pulsar 消息的反序列化器，详见[反序列化器](#反序列化器)。

### 指定消费的 Topic 或者 Topic 分区

Pulsar Source 提供了两种订阅 Topic 或 Topic 分区的方式。

- Topic 列表，从这个 Topic 的所有分区上消费消息，例如：
  {{< tabs "pulsar-source-topics" >}}
  {{< tab "Java" >}}

  ```java
  PulsarSource.builder().setTopics("some-topic1", "some-topic2");

  // 从 topic "topic-a" 的 0 和 2 分区上消费
  PulsarSource.builder().setTopics("topic-a-partition-0", "topic-a-partition-2");
  ```

  {{< /tab >}}
  {{< tab "Python" >}}

  ```python
  PulsarSource.builder().set_topics(["some-topic1", "some-topic2"])

  # 从 topic "topic-a" 的 0 和 2 分区上消费
  PulsarSource.builder().set_topics(["topic-a-partition-0", "topic-a-partition-2"])
  ```

  {{< /tab >}}
  {{< /tabs >}}

- Topic 正则，Pulsar Source 使用给定的正则表达式匹配出所有合规的 Topic，例如：
  {{< tabs "pulsar-source-topic-pattern" >}}
  {{< tab "Java" >}}

  ```java
  PulsarSource.builder().setTopicPattern("topic-*");
  ```

  {{< /tab >}}
  {{< tab "Python" >}}

  ```python
  PulsarSource.builder().set_topic_pattern("topic-*")
  ```

  {{< /tab >}}
  {{< /tabs >}}

#### Topic 名称简写

从 Pulsar 2.0 之后，完整的 Topic 名称格式为 `{persistent|non-persistent}://租户/命名空间/topic`。但是 Pulsar Source 不需要提供 Topic 名称的完整定义，因为 Topic 类型、租户、命名空间都设置了默认值。

| Topic 属性 | 默认值          |
|:---------|:-------------|
| Topic 类型 | `persistent` |
| 租户       | `public`     |
| 命名空间     | `default`    |

下面的表格提供了当前 Pulsar Topic 支持的简写方式：

| Topic 名称简写                        | 翻译后的 Topic 名称                                  |
|:----------------------------------|:-----------------------------------------------|
| `my-topic`                        | `persistent://public/default/my-topic`         |
| `my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic` |

{{< hint warning >}}
对于 Non-persistent（非持久化）Topic，Pulsar Source 不支持简写名称。所以无法将 `non-persistent://public/default/my-topic` 简写成 `non-persistent://my-topic`。
{{< /hint >}}

#### Pulsar Topic 层次结构

对于 Pulsar 而言，Topic 分区也是一种 Topic。Pulsar 会将一个有分区的 Topic 在内部按照分区的大小拆分成等量的无分区 Topic。

由于 Pulsar 内部的分区实际实现为一个 Topic，我们将用“分区”来指代“仅有一个分区的 Topic（Non-partitioned Topic）”和“具有多个分区的 Topic 下属的分区”。

例如，在 Pulsar 的 `sample` 租户下面的 `flink` 命名空间里面创建了一个有 3 个分区的 Topic，给它起名为 `simple-string`。可以在 Pulsar 上看到如下的 Topic 列表：

| Topic 名称                                              | 是否分区 |
|:------------------------------------------------------|:-----|
| `persistent://sample/flink/simple-string`             | 是    |
| `persistent://sample/flink/simple-string-partition-0` | 否    |
| `persistent://sample/flink/simple-string-partition-1` | 否    |
| `persistent://sample/flink/simple-string-partition-2` | 否    |

这意味着，用户可以用上面的子 Topic 去直接消费分区里面的数据，不需要再去基于上层的父 Topic 去消费全部分区的数据。例如：使用 `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")` 将会只消费 Topic `sample/flink/simple-string` 分区 1 和 2 里面的消息。

#### 配置 Topic 正则表达式

前面提到了 Pulsar Topic 有 `persistent`、`non-persistent` 两种类型，使用正则表达式消费数据的时候，Pulsar Source 会尝试从正则表达式里面解析出消息的类型。例如：`PulsarSource.builder().setTopicPattern("non-persistent://my-topic*")` 会解析出 `non-persistent` 这个 Topic 类型。如果用户使用 Topic 名称简写的方式，Pulsar Source 会使用默认的消息类型 `persistent`。

如果想用正则去消费 `persistent` 和 `non-persistent` 类型的 Topic，需要使用 `RegexSubscriptionMode` 定义 Topic 类型，例如：`setTopicPattern("topic-*", RegexSubscriptionMode.AllTopics)`。

### 反序列化器

反序列化器用于解析 Pulsar 消息，Pulsar Source 使用 `PulsarDeserializationSchema` 来定义反序列化器。用户可以在 builder 类中使用 `setDeserializationSchema(PulsarDeserializationSchema)` 方法配置反序列化器。

如果用户只关心消息体的二进制字节流，并不需要其他属性来解析数据。可以直接使用预定义的 `PulsarDeserializationSchema`。Pulsar Source里面提供了 3 种预定义的反序列化器。

- 使用 Pulsar 的 [Schema](https://pulsar.apache.org/docs/2.10.x/schema-understand/) 解析消息。如果使用 KeyValue 或者 Struct 类型的 Schema, 那么 Pulsar 的 `Schema` 将不会含有类型类信息， 但 `PulsarSchemaTypeInformation` 需要通过传入类型类信息来构造。因此我们提供的 API 支持用户传入类型信息。
  ```java
  // 基础数据类型
  PulsarDeserializationSchema.pulsarSchema(Schema);

  // 结构类型 (JSON, Protobuf, Avro, etc.)
  PulsarDeserializationSchema.pulsarSchema(Schema, Class);

  // 键值对类型
  PulsarDeserializationSchema.pulsarSchema(Schema, Class, Class);
  ```
- 使用 Flink 的 `DeserializationSchema` 解析消息。
  {{< tabs "pulsar-deserializer-deserialization-schema" >}}
  {{< tab "Java" >}}
  ```java
  PulsarDeserializationSchema.flinkSchema(DeserializationSchema);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  PulsarDeserializationSchema.flink_schema(DeserializationSchema)
  ```
  {{< /tab >}}
  {{< /tabs >}}

- 使用 Flink 的 `TypeInformation` 解析消息。
  {{< tabs "pulsar-deserializer-type-information" >}}
  {{< tab "Java" >}}
  ```java
  PulsarDeserializationSchema.flinkTypeInfo(TypeInformation, ExecutionConfig);
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  PulsarDeserializationSchema.flink_type_info(TypeInformation)
  ```
  {{< /tab >}}
  {{< /tabs >}}

Pulsar 的 `Message<byte[]>` 包含了很多 [额外的属性](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#messages)。例如，消息的 key、消息发送时间、消息生产时间、用户在消息上自定义的键值对属性等。可以使用 `Message<byte[]>` 接口来获取这些属性。

如果用户需要基于这些额外的属性来解析一条消息，可以实现 `PulsarDeserializationSchema` 接口。并一定要确保 `PulsarDeserializationSchema.getProducedType()` 方法返回的 `TypeInformation` 是正确的结果。Flink 使用 `TypeInformation` 将解析出来的结果序列化传递到下游算子。

同时使用 `PulsarDeserializationSchema.pulsarSchema()` 以及在 builder 中指定 `PulsarSourceBuilder.enableSchemaEvolution()` 可以启用 [Schema evolution][schema-evolution] 特性。该特性会使用 Pulsar Broker 端提供的 Schema 版本兼容性检测以及 Schema 版本演进。下列示例展示了如何启用 Schema Evolution。

```java
Schema<SomePojo> schema = Schema.AVRO(SomePojo.class);

PulsarSource<SomePojo> source = PulsarSource.builder()
    ...
    .setDeserializationSchema(schema, SomePojo.class)
    .enableSchemaEvolution()
    .build();
```

如果使用 Pulsar 原生的 Schema 来反序列化消息却不启用 Schema Evolution 特性，我们将会跳过 Schema 兼容性检查，解析一些消息时可能会遇到未知的错误。

### 定义 RangeGenerator

如果想在 Pulsar Source 里面使用 `Key_Shared` 订阅，需要提供 `RangeGenerator` 实例。`RangeGenerator` 会生成一组消息 key 的 hash 范围，Pulsar Source 会基于给定的范围来消费数据。

由于 Pulsar 并未提供 Key 的 Hash 计算方法，所以我们在 Flink 中提供了名为 `FixedKeysRangeGenerator` 的实现，你可以在 builder 中依次提供需要消费的 Key 内容即可。但需要注意的是，Pulsar 的 Key Hash 值并不对应唯一的一个 Key，所以如果你只想消费某几个 Key 的消息，还需要在后面的代码中使用 `DataStream.filter()` 方法来过滤出对应的消息。

### 起始消费位置

Pulsar Source 使用 `setStartCursor(StartCursor)` 方法给定开始消费的位置。内置的开始消费位置有：

- 从 Topic 里面最早的一条消息开始消费。
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

- 从 Topic 里面最新的一条消息开始消费。
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

- 从给定的消息开始消费。
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

- 与前者不同的是，给定的消息可以跳过，再进行消费。
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

- 从给定的消息发布时间开始消费，这个方法因为名称容易导致误解现在已经不建议使用。你可以使用方法 `StartCursor.fromPublishTime(long)`。
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

- 从给定的消息发布时间开始消费。
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

{{< hint info >}}
每条消息都有一个固定的序列号，这个序列号在 Pulsar 上有序排列，其包含了 ledger、entry、partition 等原始信息，用于在 Pulsar 底层存储上查找到具体的消息。

Pulsar 称这个序列号为 `MessageId`，用户可以使用 `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)` 创建它。
{{< /hint >}}

### 边界

Pulsar Source 默认情况下使用流的方式消费数据。除非任务失败或者被取消，否则将持续消费数据。用户可以使用 `setBoundedStopCursor(StopCursor)` 给定停止消费的位置，这种情况下会使用批的方式进行消费。使用流的方式一样可以给定停止位置，使用 `setUnboundedStopCursor(StopCursor)` 方法即可。

在批模式下，使用 `setBoundedStopCursor(StopCursor)` 来指定一个消费停止位置。

内置的停止消费位置如下：

- 永不停止。
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

- 停止于 Pulsar 启动时 Topic 里面最新的那条数据。
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

- 停止于某条消息，结果里不包含此消息。
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

- 停止于某条消息之后，结果里包含此消息。
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

- 停止于某个给定的消息事件时间戳，比如 `Message<byte[]>.getEventTime()`，消费结果里不包含此时间戳的消息。
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

- 停止于某个给定的消息事件时间戳，比如 `Message<byte[]>.getEventTime()`，消费结果里包含此时间戳的消息。
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

- 停止于某个给定的消息发布时间戳，比如 `Message<byte[]>.getPublishTime()`，消费结果里不包含此时间戳的消息。
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

- 停止于某个给定的消息发布时间戳，比如 `Message<byte[]>.getPublishTime()`，消费结果里包含此时间戳的消息。
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

### Source 配置项

除了前面提到的配置选项，Pulsar Source 还提供了丰富的选项供 Pulsar 专家使用，在 builder 类里通过 `setConfig(ConfigOption<T>, T)` 和 `setConfig(Configuration)` 方法给定下述的全部配置。

#### Pulsar Java 客户端配置项

Pulsar Source 使用 [Java 客户端](https://pulsar.apache.org/docs/2.10.x/client-libraries-java/)来创建消费实例，相关的配置定义于 Pulsar 的 `ClientConfigurationData` 内。在 `PulsarOptions` 选项中，定义大部分的可供用户定义的配置。

{{< generated/pulsar_client_configuration >}}

#### Pulsar 管理 API 配置项

[管理 API](https://pulsar.apache.org/docs/2.10.x/admin-api-overview/) 用于查询 Topic 的元数据和用正则订阅的时候的 Topic 查找，它与 Java 客户端共享大部分配置。下面列举的配置只供管理 API 使用，`PulsarOptions` 包含了这些配置 。

{{< generated/pulsar_admin_configuration >}}

#### Pulsar 消费者 API 配置项

Pulsar 提供了消费者 API 和读者 API 两套 API 来进行数据消费，它们可用于不同的业务场景。Flink 上的 Pulsar Source 使用消费者 API 进行消费，它的配置定义于 Pulsar 的 `ConsumerConfigurationData` 内。Pulsar Source 将其中大部分的可供用户定义的配置定义于 `PulsarSourceOptions` 内。

{{< generated/pulsar_consumer_configuration >}}

#### Pulsar Source配置项

下述配置主要用于性能调优或者是控制消息确认的行为。如非必要，可以不用强制配置。

{{< generated/pulsar_source_configuration >}}

### 动态分区发现

为了能在启动 Flink 任务之后还能发现在 Pulsar 上扩容的分区或者是新创建的 Topic，Pulsar Source 提供了动态分区发现机制。该机制不需要重启 Flink 任务。对选项 `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS` 设置一个正整数即可启用。

{{< tabs "pulsar-dynamic-partition-discovery" >}}
{{< tab "Java" >}}

```java
// 10 秒查询一次分区信息
PulsarSource.builder()
        .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```

{{< /tab >}}
{{< tab "Python" >}}

```python
# 10 秒查询一次分区信息
PulsarSource.builder()
    .set_config("pulsar.source.partitionDiscoveryIntervalMs", 10000)
```

{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
- 默认情况下，Pulsar 启用分区发现，查询间隔为 5 分钟。用户可以给定一个负数，将该功能禁用。如果使用批的方式消费数据，将无法启用该功能。
- 如果需要禁用分区发现功能，你需要将查询间隔设置为负值。
- 在 bounded 消费模式下，即使将分区发现的查询间隔设置为正值，也会被禁用。
{{< /hint >}}

### 事件时间和水位线

默认情况下，Pulsar Source 使用 Pulsar 的 `Message<byte[]>` 里面的时间作为解析结果的时间戳。用户可以使用 `WatermarkStrategy` 来自行解析出想要的消息时间，并向下游传递对应的水位线。

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

[这篇文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}})详细讲解了如何定义 `WatermarkStrategy`。

### 消息确认

一旦在 Topic 上创建了订阅，消息便会[存储](https://pulsar.apache.org/docs/2.10.x/concepts-architecture-overview/#persistent-storage)在 Pulsar 里。即使没有消费者，消息也不会被丢弃。只有当 Flink 同 Pulsar 确认此条消息已经被消费，该消息才以某种机制会被移除。

我们使用 `独占` 作为默认的订阅模式。此订阅下，Pulsar Source 使用累进式确认方式。确认某条消息已经被处理时，其前面消息会自动被置为已读。Pulsar Source 会在 Flink 完成检查点时将对应时刻消费的消息置为已读，以此来保证 Pulsar 状态与 Flink 状态一致。

如果用户没有在 Flink 上启用检查点，Pulsar Source 可以使用周期性提交来将消费状态提交给 Pulsar，使用配置 `PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL` 来进行定义。

需要注意的是，此种场景下，Pulsar Source 并不依赖于提交到 Pulsar 的状态来做容错。消息确认只是为了能在 Pulsar 端看到对应的消费处理情况。

## Pulsar Sink

Pulsar Sink 连接器可以将经过 Flink 处理后的数据写入一个或多个 Pulsar Topic 或者 Topic 下的某些分区。

{{< hint info >}}
Pulsar Sink 基于 Flink 最新的 [Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) 实现。

如果想要使用旧版的使用 `SinkFunction` 接口实现的 Sink 连接器，可以使用 StreamNative 维护的 [pulsar-flink](https://github.com/streamnative/pulsar-flink)。
{{< /hint >}}

### 使用示例

Pulsar Sink 使用 builder 类来创建 `PulsarSink` 实例。

下面示例展示了如何通过 Pulsar Sink 以“至少一次”的语义将字符串类型的数据发送给 topic1。

{{< tabs "pulsar-sink-example" >}}
{{< tab "Java" >}}

```java
DataStream<String> stream = ...

PulsarSink<String> sink = PulsarSink.builder()
    .setServiceUrl(serviceUrl)
    .setAdminUrl(adminUrl)
    .setTopics("topic1")
    .setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
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
    .set_serialization_schema(PulsarSerializationSchema.flink_schema(SimpleStringSchema())) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

stream.sink_to(pulsar_sink)
```

{{< /tab >}}
{{< /tabs >}}

下列为创建一个 `PulsarSink` 实例必需的属性：

- Pulsar 数据消费的地址，使用 `setServiceUrl(String)` 方法提供。
- Pulsar HTTP 管理地址，使用 `setAdminUrl(String)` 方法提供。
- 需要发送到的 Topic 或者是 Topic 下面的分区，详见[指定写入的 Topic 或者 Topic 分区](#指定写入的-topic-或者-topic-分区)。
- 编码 Pulsar 消息的序列化器，详见[序列化器](#序列化器)。

在创建 `PulsarSink` 时，建议使用 `setProducerName(String)` 来指定 `PulsarSink` 内部使用的 Pulsar 生产者名称。这样方便在数据监控页面找到对应的生产者监控指标。

### 指定写入的 Topic 或者 Topic 分区

`PulsarSink` 指定写入 Topic 的方式和 Pulsar Source [指定消费的 Topic 或者 Topic 分区](#指定消费的-topic-或者-topic-分区)的方式类似。`PulsarSink` 支持以 mixin 风格指定写入的 Topic 或分区。因此，可以指定一组 Topic 或者分区或者是两者都有。

{{< tabs "set-pulsar-sink-topics" >}}
{{< tab "Java" >}}

```java
// Topic "some-topic1" 和 "some-topic2"
PulsarSink.builder().setTopics("some-topic1", "some-topic2")

// Topic "topic-a" 的分区 0 和 2
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")

// Topic "topic-a" 以及 Topic "some-topic2" 分区 0 和 2
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2", "some-topic2")
```

{{< /tab >}}
{{< tab "Python" >}}

```python
# Topic "some-topic1" 和 "some-topic2"
PulsarSink.builder().set_topics(["some-topic1", "some-topic2"])

# Topic "topic-a" 的分区 0 和 2
PulsarSink.builder().set_topics(["topic-a-partition-0", "topic-a-partition-2"])

# Topic "topic-a" 以及 Topic "some-topic2" 分区 0 和 2
PulsarSink.builder().set_topics(["topic-a-partition-0", "topic-a-partition-2", "some-topic2"])
```

{{< /tab >}}
{{< /tabs >}}

动态分区发现默认处于开启状态，这意味着 `PulsarSink` 将会周期性地从 Pulsar 集群中查询 Topic 的元数据来获取可能有的分区数量变更信息。使用 `PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` 配置项来指定查询的间隔时间。

可以选择实现 `TopicRouter` 接口来自定义[消息路由策略](#消息路由策略)。此外，阅读 [Topic 名称简写](#topic-名称简写)将有助于理解 Pulsar 的分区在 Pulsar 连接器中的配置方式。

{{< hint warning >}}
如果在 `PulsarSink` 中同时指定了某个 Topic 和其下属的分区，那么 `PulsarSink` 将会自动将两者合并，仅使用外层的 Topic。

举个例子，如果通过 `PulsarSink.builder().setTopics("some-topic1", "some-topic1-partition-0")` 来指定写入的 Topic，那么其结果等价于 `PulsarSink.builder().setTopics("some-topic1")`。
{{< /hint >}}

#### 基于消息实例的动态 Topic 指定

除了前面说的一开始就指定 Topic 或者是 Topic 分区，你还可以在程序启动后基于消息内容动态指定 Topic，只需要实现 `TopicRouter` 接口即可。使用 `PulsarSinkContext.topicMetadata(String)` 方法来查询某个 Topic 在 Pulsar 上有多少个分区，查询结果会缓存并在 `PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` 毫秒之后失效。

此方法同样支持将消息写入一个不存在的 Topic，可以在 `TopicRouter` 内返回想要创建的 Topic，连接器将会**尝试**创建。

{{< hint warning >}}
如果使用 Topic 自动创建功能，需要在 Pulsar 的 `broker.conf` 配置文件内配置 `allowAutoTopicCreation=true` 来启用对应的功能。

`broker.conf` 配置文件的 `allowAutoTopicCreationType` 选项可以控制自动创建的 Topic 的类型。

- `non-partitioned`: 默认配置，创建的 Topic 没有分区，并且不可以手动创建分区。
- `partitioned`: 创建的 Topic 将按照 `defaultNumPartitions` 选项定义的个数创建对应的分区。
{{< /hint >}}

### 序列化器

序列化器（`PulsarSerializationSchema`）负责将 Flink 中的每条记录序列化成 byte 数组，并通过网络发送至指定的写入 Topic。和 Pulsar Source 类似的是，序列化器同时支持使用基于 Flink 的 `SerializationSchema` 接口实现序列化器和使用 Pulsar 原生的 `Schema` 类型实现的序列化器。不过序列化器并不支持 Pulsar 的 `Schema.AUTO_PRODUCE_BYTES()`。

如果不需要指定 [Message](https://pulsar.apache.org/api/client/2.10.x/org/apache/pulsar/client/api/Message.html) 接口中提供的 key 或者其他的消息属性，可以从上述 2 种预定义的 `PulsarSerializationSchema` 实现中选择适合需求的一种使用。

- 使用 Pulsar 的 [Schema](https://pulsar.apache.org/docs/2.10.x/schema-understand/) 来序列化 Flink 中的数据。
  ```java
  // 原始数据类型
  PulsarSerializationSchema.pulsarSchema(Schema)

  // 有结构数据类型（JSON、Protobuf、Avro 等）
  PulsarSerializationSchema.pulsarSchema(Schema, Class)

  // 键值对类型
  PulsarSerializationSchema.pulsarSchema(Schema, Class, Class)
  ```
- 使用 Flink 的 `SerializationSchema` 来序列化数据。

  {{< tabs "set-pulsar-serialization-flink-schema" >}}
  {{< tab "Java" >}}

  ```java
  PulsarSerializationSchema.flinkSchema(SerializationSchema)
  ```

  {{< /tab >}}
  {{< tab "Python" >}}

  ```python
  PulsarSerializationSchema.flink_schema(SimpleStringSchema())
  ```

  {{< /tab >}}
  {{< /tabs >}}

#### Schema evolution

同时使用 `PulsarSerializationSchema.pulsarSchema()` 以及在 builder 中指定 `PulsarSinkBuilder.enableSchemaEvolution()` 可以启用 [Schema evolution][schema-evolution] 特性。该特性会使用 Pulsar Broker 端提供的 Schema 版本兼容性检测以及 Schema 版本演进。下列示例展示了如何启用 Schema Evolution。

```java
Schema<SomePojo> schema = Schema.AVRO(SomePojo.class);
PulsarSerializationSchema<SomePojo> pulsarSchema = PulsarSerializationSchema.pulsarSchema(schema, SomePojo.class);

PulsarSink<String> sink = PulsarSink.builder()
    ...
    .setSerializationSchema(pulsarSchema)
    .enableSchemaEvolution()
    .build();
```

{{< hint warning >}}
如果想要使用 Pulsar 原生的 Schema 序列化消息而不需要 Schema Evolution 特性，那么写入的 Topic 会使用 `Schema.BYTES` 作为消息的 Schema，对应 Topic 的消费者需要自己负责反序列化的工作。

例如，如果使用 `PulsarSerializationSchema.pulsarSchema(Schema.STRING)` 而不使用 `PulsarSinkBuilder.enableSchemaEvolution()`。那么在写入 Topic 中所记录的消息 Schema 将会是 `Schema.BYTES`。
{{< /hint >}}

#### PulsarMessage<byte[]> 类型的消息的校验

Pulsar 的 topic 至少会包含一种 Schema，`Schema.BYTES` 是默认的 Schema 类型并常作为没有 Schema 的 topic 的 Schema 类型。使用 `Schema.BYTES` 发送消息将会跳过类型检测，这意味着使用 `SerializationSchema` 和没有启用 Schema evolution 的 `Schema` 所发送的消息并不安全。

可以启用 `pulsar.sink.validateSinkMessageBytes` 选项来让链接器使用 Pulsar 提供的 `Schema.AUTO_PRODUCE_BYTES()` 发送消息。它会在发送字节数组消息时额外进行校验，与 topic 上最新版本的 Schema 进行对比，保证消息内容能正确。

但是，并非所有的 Pulsar 的 Schema 都支持校验字符串，所以在默认情况下我们禁用了此选项。可以按需启用。

#### 自定义序列化器

可以通过继承 `PulsarSerializationSchema` 接口来实现自定义的序列化逻辑。接口需要返回一个类型为 `PulsarMessage` 的消息，此类型实例无法被直接创建，连接器提供了构造方法并定义了三种消息类型的构建。

- 使用 Pulsar 的 `Scheme` 来构建消息，常用于你知道 topic 对应的 schema 是什么的时候。我们会检查你提供的 `Schema` 在 topic 上是否兼容。
  ```java
  PulsarMessage.builder(Schema<M> schema, M message)
      ...
      .build();
  ```
- 创建一个消息类型为字节数组的消息。默认情况下不进行 Schema 检查。
  ```java
  PulsarMessage.builder(byte[] bytes)
      ...
      .build();
  ```
- 创建一个消息体为空的墓碑消息。[墓碑][tombstone-data-store] 是一种特殊的消息，并在 Pulsar 中提供了支持。
  ```java
  PulsarMessage.builder()
      ...
      .build();
  ```

### 消息路由策略

在 Pulsar Sink 中，消息路由发生在于分区之间，而非上层 Topic。对于给定 Topic 的情况，路由算法会首先会查询出 Topic 之上所有的分区信息，并在这些分区上实现消息的路由。Pulsar Sink 默认提供 2 种路由策略的实现。

- `KeyHashTopicRouter`：使用消息的 key 对应的哈希值来取模计算出消息对应的 Topic 分区。

  使用此路由可以将具有相同 key 的消息发送至同一个 Topic 分区。消息的 key 可以在自定义 `PulsarSerializationSchema` 时，在 `serialize()` 方法内使用 `PulsarMessageBuilder.key(String key)` 来予以指定。

  如果消息没有包含 key，此路由策略将从 Topic 分区中随机选择一个发送。

  可以使用 `MessageKeyHash.JAVA_HASH` 或者 `MessageKeyHash.MURMUR3_32_HASH` 两种不同的哈希算法来计算消息 key 的哈希值。使用 `PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH` 配置项来指定想要的哈希算法。

- `RoundRobinRouter`：轮换使用用户给定的 Topic 分区。

  消息将会轮替地选取 Topic 分区，当往某个 Topic 分区里写入指定数量的消息后，将会轮换至下一个 Topic 分区。使用 `PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES` 指定向一个 Topic 分区中写入的消息数量。

还可以通过实现 `TopicRouter` 接口来自定义消息路由策略，请注意 TopicRouter 的实现需要能被序列化。

在 `TopicRouter` 内可以指定任意的 Topic 分区（即使这个 Topic 分区不在 `setTopics()` 指定的列表中）。因此，当使用自定义的 `TopicRouter` 时，`PulsarSinkBuilder.setTopics` 选项是可选的。

```java
@PublicEvolving
public interface TopicRouter<IN> extends Serializable {

    TopicPartition route(IN in, List<TopicPartition> partitions, PulsarSinkContext context);

    default void open(SinkConfiguration sinkConfiguration) {
        // 默认无操作
    }
}
```

{{< hint info >}}
如前文所述，Pulsar 分区的内部被实现为一个无分区的 Topic，一般情况下 Pulsar 客户端会隐藏这个实现，并且提供内置的消息路由策略。Pulsar Sink 并没有使用 Pulsar 客户端提供的路由策略和封装，而是使用了 Pulsar 客户端更底层的 API 自行实现了消息路由逻辑。这样做的主要目的是能够在属于不同 Topic 的分区之间定义更灵活的消息路由策略。

详情请参考 Pulsar 的 [partitioned topics](https://pulsar.apache.org/docs/2.10.x/cookbooks-partitioned/) 文档。
{{< /hint >}}

### 发送一致性

`PulsarSink` 支持三种发送一致性。

- `NONE`：Flink 应用运行时可能出现数据丢失的情况。在这种模式下，Pulsar Sink 发送消息后并不会检查消息是否发送成功。此模式具有最高的吞吐量，可用于一致性没有要求的场景。
- `AT_LEAST_ONCE`：每条消息**至少有**一条对应消息发送至 Pulsar，发送至 Pulsar 的消息可能会因为 Flink 应用重启而出现重复。
- `EXACTLY_ONCE`：每条消息**有且仅有**一条对应消息发送至 Pulsar。发送至 Pulsar 的消息不会有重复也不会丢失。Pulsar Sink 内部依赖 [Pulsar 事务](https://pulsar.apache.org/docs/2.10.x/transactions/)和两阶段提交协议来保证每条记录都能正确发往 Pulsar。

{{< hint warning >}}
如果想要使用 `EXACTLY_ONCE`，需要用户确保在 Flink 程序上启用 checkpoint，同时在 Pulsar 上启用事务。在此模式下，Pulsar sink 会将消息写入到某个未提交的事务下，并在成功执行完 checkpoint 后提交对应的事务。

基于 Pulsar 的设计，任何在开启的事务之后写入的消息是无法被消费到的。只有这个事务提交了，对应的消息才能被消费到。
{{< /hint >}}

### 消息延时发送

[消息延时发送](https://pulsar.apache.org/docs/2.10.x/concepts-messaging/#delayed-message-delivery)特性可以让指定发送的每一条消息需要延时一段时间后才能被下游的消费者所消费。当延时消息发送特性启用时，Pulsar Sink 会**立刻**将消息发送至 Pulsar Broker。但该消息在指定的延迟时间到达前将会保持对下游消费者不可见。

消息延时发送仅在 `Shared` 订阅模式下有效，在 `Exclusive` 和 `Failover` 模式下该特性无效。

可以使用 `MessageDelayer.fixed(Duration)` 创建一个 `MessageDelayer` 来为所有消息指定恒定的接收时延，或者实现 `MessageDelayer` 接口来为不同的消息指定不同的接收时延。

{{< hint warning >}}
消息对下游消费者的可见时间应当基于 `PulsarSinkContext.processTime() `计算得到。
{{< /hint >}}

### Sink 配置项

可以在 builder 类里通过 `setConfig(ConfigOption<T>, T)` 和 `setConfig(Configuration)` 方法给定下述的全部配置。

#### PulsarClient 和 PulsarAdmin 配置项

Pulsar Sink 和 Pulsar Source 公用的配置选项可参考

- [Pulsar Java 客户端配置项](#pulsar-java-客户端配置项)
- [Pulsar 管理 API 配置项](#pulsar-管理-api-配置项)

#### Pulsar 生产者 API 配置项

Pulsar Sink 使用生产者 API 来发送消息。Pulsar 的 `ProducerConfigurationData` 中大部分的配置项被映射为 `PulsarSinkOptions` 里的选项。

{{< generated/pulsar_producer_configuration >}}

#### Pulsar Sink 配置项

下述配置主要用于性能调优或者是控制消息确认的行为。如非必要，可以不用考虑配置。

{{< generated/pulsar_sink_configuration >}}

### 设计思想简述

Pulsar Sink 遵循 [FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) 中定义的 Sink API 设计。

#### 无状态的 SinkWriter

在 `EXACTLY_ONCE` 一致性下，Pulsar Sink 不会将事务相关的信息存放于检查点快照中。这意味着当 Flink 应用重启时，Pulsar Sink 会创建新的事务实例。上一次运行过程中任何未提交事务中的消息会因为超时中止而无法被下游的消费者所消费。这样的设计保证了 SinkWriter 是无状态的。

#### Pulsar Schema Evolution

[Pulsar Schema Evolution][schema-evolution] 允许用户在一个 Flink 应用程序中使用的数据模型发生特定改变后（比如向基于 ARVO 的 POJO 类中增加或删除一个字段），仍能使用同一个 Flink 应用程序的代码。

可以在 Pulsar 集群内指定哪些类型的数据模型的改变是被允许的，详情请参阅 [Pulsar Schema Evolution][schema-evolution]。

## 监控指标

默认情况下，Pulsar client 每隔 60 秒才会刷新一次监控数据。如果想要提高刷新频率，可以通过如下方式来将 Pulsar client 的监控数据刷新频率调整至相应值（最低为1s）：

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

### Source 监控指标

在 [FLIP-33: Standardize Connector Metrics][standard-metrics] 定义的基础 Source 指标之上，我们额外提供了一些来自 Client 的监控指标。你需要启用 `pulsar.source.enableMetrics` 选项来获得这些监控指标，所有的指标列举在下面的表格中。

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

| 指标                                                           | 变量                | 描述                                       | 类型  |
| -------------------------------------------------------------- | ------------------- | ------------------------------------------ | ----- |
| PulsarConsumer."Topic"."ConsumerName".numMsgsReceived          | Topic, ConsumerName | 在过去的一个统计窗口内消费的消息数         | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numBytesReceived         | Topic, ConsumerName | 在过去的一个统计窗口内消费的字节数         | Gauge |
| PulsarConsumer."Topic"."ConsumerName".rateMsgsReceived         | Topic, ConsumerName | 在过去的一个统计窗口内消费的消息速率       | Gauge |
| PulsarConsumer."Topic"."ConsumerName".rateBytesReceived        | Topic, ConsumerName | 在过去的一个统计窗口内消费的字节速率       | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numAcksSent              | Topic, ConsumerName | 在过去的一个统计窗口内确认消费成功的消息数 | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numAcksFailed            | Topic, ConsumerName | 在过去的一个统计窗口内确认消费失败的消息数 | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numReceiveFailed         | Topic, ConsumerName | 在过去的一个统计窗口内消费失败的消息数     | Gauge |
| PulsarConsumer."Topic"."ConsumerName".numBatchReceiveFailed    | Topic, ConsumerName | 在过去的一个统计窗口内批量消费失败的消息数 | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalMsgsReceived        | Topic, ConsumerName | Consumer 消费的全部消息数                  | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalBytesReceived       | Topic, ConsumerName | Consumer 消费的全部字节数                  | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalReceivedFailed      | Topic, ConsumerName | Consumer 消费失败的消息数                  | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalBatchReceivedFailed | Topic, ConsumerName | Consumer 批量消费失败的消息数              | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalAcksSent            | Topic, ConsumerName | Consumer 确认消费成功的消息数              | Gauge |
| PulsarConsumer."Topic"."ConsumerName".totalAcksFailed          | Topic, ConsumerName | Consumer 确认消费失败的消息数              | Gauge |
| PulsarConsumer."Topic"."ConsumerName".msgNumInReceiverQueue    | Topic, ConsumerName | Consumer 当前待消费的消息队列大小          | Gauge |

### Sink 监控指标

下列表格列出了当前 Sink 支持的监控指标，前 6 个指标是 [FLIP-33: Standardize Connector Metrics][standard-metrics] 中规定的 Sink 连接器应当支持的标准指标。前 5 个指标会默认暴露给用户，其他指标需要通过启用 `pulsar.sink.enableMetrics` 选项来获得。

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

| 指标                                                          | 变量                | 描述                                                   | 类型    |
| ------------------------------------------------------------- | ------------------- | ------------------------------------------------------ | ------- |
| numBytesOut                                                   | n/a                 | Pulsar Sink 启动后总共发出的字节数                     | Counter |
| numBytesOutPerSecond                                          | n/a                 | 每秒发送的字节数                                       | Meter   |
| numRecordsOut                                                 | n/a                 | Pulsar Sink 启动后总共发出的消息数                     | Counter |
| numRecordsOutPerSecond                                        | n/a                 | 每秒发送的消息数                                       | Meter   |
| numRecordsOutErrors                                           | n/a                 | 总共发送消息失败的次数                                 | Counter |
| currentSendTime                                               | n/a                 | 最近一条消息从被放入客户端缓冲队列到收到消息确认的时间 | Gauge   |
| PulsarProducer."Topic"."ProducerName".numMsgsSent             | Topic, ProducerName | 在过去的一个统计窗口内发送的消息数                     | Gauge   |
| PulsarProducer."Topic"."ProducerName".numBytesSent            | Topic, ProducerName | 在过去的一个统计窗口内发送的字节数                     | Gauge   |
| PulsarProducer."Topic"."ProducerName".numSendFailed           | Topic, ProducerName | 在过去的一个统计窗口内发送失败的消息数                 | Gauge   |
| PulsarProducer."Topic"."ProducerName".numAcksReceived         | Topic, ProducerName | 在过去的一个统计窗口内总共收到的确认数                 | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendMsgsRate            | Topic, ProducerName | 在过去的一个统计窗口内发送的消息速率                   | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendBytesRate           | Topic, ProducerName | 在过去的一个统计窗口内发送的字节速率                   | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis50pct  | Topic, ProducerName | 在过去的一个统计窗口内的发送延迟的中位数               | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis75pct  | Topic, ProducerName | 在过去的一个统计窗口内的发送延迟的 75 百分位数         | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis95pct  | Topic, ProducerName | 在过去的一个统计窗口内的发送延迟的 95 百分位数         | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis99pct  | Topic, ProducerName | 在过去的一个统计窗口内的发送延迟的 99 百分位数         | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillis999pct | Topic, ProducerName | 在过去的一个统计窗口内的发送延迟的 99.9 百分位数       | Gauge   |
| PulsarProducer."Topic"."ProducerName".sendLatencyMillisMax    | Topic, ProducerName | 在过去的一个统计窗口内的最大发送延迟                   | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalMsgsSent           | Topic, ProducerName | Producer 发送的全部消息数                              | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalBytesSent          | Topic, ProducerName | Producer 发送的全部字节数                              | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalSendFailed         | Topic, ProducerName | Producer 发送失败的消息数                              | Gauge   |
| PulsarProducer."Topic"."ProducerName".totalAcksReceived       | Topic, ProducerName | Producer 确认发送成功的消息数                          | Gauge   |
| PulsarProducer."Topic"."ProducerName".pendingQueueSize        | Topic, ProducerName | Producer 当前待发送的消息队列大小                      | Gauge   |

{{< hint info >}}
- 指标 `numBytesOut`、`numRecordsOut` 和 `numRecordsOutErrors` 从 Pulsar client 实例的监控指标中获得。

- `numBytesOutRate` 和 `numRecordsOutRate` 指标是 Flink 内部通过 `numBytesOut` 和 `numRecordsOut` 计数器，在一个 60 秒的窗口内计算得到的。

- `currentSendTime` 记录了最近一条消息从放入生产者的缓冲队列到消息被消费确认所耗费的时间。这项指标在 `NONE` 发送一致性下不可用。
{{< /hint >}}

## 端到端加密

Flink 可以使用 Pulsar 提供的加解密功能在 Source 和 Sink 端加解密消息。用户需要提供一个合法的密钥对（即一个公钥和一个私钥，也就是非对称加密方式）来实现端到端的加密。

### 如何启用端到端加密

1. 创建密钥对

   Pulsar 同时支持 ECDSA 或者 RSA 密钥对，你可以同时创建多组不同类型的密钥对，加密消息时会选择其中任意一组密钥来确保消息更加安全。
   ```shell
   # ECDSA（仅用于 Java 端）
   openssl ecparam -name secp521r1 -genkey -param_enc explicit -out test_ecdsa_privkey.pem
   openssl ec -in test_ecdsa_privkey.pem -pubout -outform pem -out test_ecdsa_pubkey.pem

   # RSA
   openssl genrsa -out test_rsa_privkey.pem 2048
   openssl rsa -in test_rsa_privkey.pem -pubout -outform pkcs8 -out test_rsa_pubkey.pem
   ```

2. 实现 `CryptoKeyReader` 接口

   每个密钥对都需要有一个唯一的密钥名称，用户需要自行实现 `CryptoKeyReader` 接口并确保 `CryptoKeyReader.getPublicKey()` 和 `CryptoKeyReader.getPrivateKey()` 方法能基于给定的密钥名称反正正确的密钥。

   Pulsar 提供了一个默认的 `CryptoKeyReader` 实现 `DefaultCryptoKeyReader`。用户需要使用对于的 builder 方法 `DefaultCryptoKeyReader.builder()` 来创建实例。需要注意的是，对应的密钥对文件需要放在 Flink 程序的运行环境上。

   ```java
   // defaultPublicKey 和 defaultPrivateKey 也需要提供。
   // 文件 file:///path/to/default-public.key 需要在 Flink 的运行环境上存在。
   CryptoKeyReader keyReader = DefaultCryptoKeyReader.builder()
       .defaultPublicKey("file:///path/to/default-public.key")
       .defaultPrivateKey("file:///path/to/default-private.key")
       .publicKey("key1", "file:///path/to/public1.key").privateKey("key1", "file:///path/to/private1.key")
       .publicKey("key2", "file:///path/to/public2.key").privateKey("key2", "file:///path/to/private2.key")
       .build();
   ```

3. （可选）实现 `MessageCrypto<MessageMetadata, MessageMetadata>` 接口


   Pulsar 原生支持 **ECDSA**、**RSA** 等常见非对称加解密方法。通常情况下，你不需要实现此接口，除非你想使用一个私有的加解密方法。你可以参考 Pulsar 的默认实现 `MessageCryptoBc` 来实现 `MessageCrypto<MessageMetadata, MessageMetadata>` 接口。

4. 创建 `PulsarCrypto` 实例

   `PulsarCrypto` 用于提供所有必要的加解密信息，你可以使用对应的 builder 方法来创建实例。

   ```java
   CryptoKeyReader keyReader = DefaultCryptoKeyReader.builder()
       .defaultPublicKey("file:///path/to/public1.key")
       .defaultPrivateKey("file:///path/to/private2.key")
       .publicKey("key1", "file:///path/to/public1.key").privateKey("key1", "file:///path/to/private1.key")
       .publicKey("key2", "file:///path/to/public2.key").privateKey("key2", "file:///path/to/private2.key")
       .build();

   // 此处只用于演示如何使用，实际上你不需要这么做。
   SerializableSupplier<MessageCrypto<MessageMetadata, MessageMetadata>> cryptoSupplier = () -> new MessageCryptoBc();

   PulsarCrypto pulsarCrypto = PulsarCrypto.builder()
       .cryptoKeyReader(keyReader)
       // 所有的密钥名称需要在此处给出。
       .addEncryptKeys("key1", "key2")
       // 一般情况下你不需要提供 MessageCrypto 实例。
       .messageCrypto(cryptoSupplier)
       .build()
   ```

### 在 Pulsar source 上解密消息

基于前面的指导创建对应的 `PulsarCrypto` 实例，然后在 `PulsarSource.builder()` 的构造方法里面予以给定。你需要同时定义解密失败的行为，Pulsar 在 `ConsumerCryptoFailureAction` 给定了 3 种实现。

- `ConsumerCryptoFailureAction.FAIL`: Flink 程序将抛出异常并退出。
- `ConsumerCryptoFailureAction.DISCARD`: 解密失败的消息将被丢弃。
- `ConsumerCryptoFailureAction.CONSUME`

  解密失败的消息将以未解密的状态传递给后续的算子，你也可以在 `PulsarDeserializationSchema` 里手动对解密失败的消息进行再次解密。所有关于解密的上下文都定义在 `Message.getEncryptionCtx()` 内。

```java
PulsarCrypto pulsarCrypto = ...

PulsarSource<String> sink = PulsarSource.builder()
    ...
    .setPulsarCrypto(pulsarCrypto, ConsumerCryptoFailureAction.FAIL)
    .build();
```

### 在 Pulsar sink 上加密消息

基于前面的指导创建对应的 `PulsarCrypto` 实例，然后在 `PulsarSink.builder()` 的构造方法里面予以给定。你需要同时定义加密失败的行为，Pulsar 在 `ProducerCryptoFailureAction` 给定了 2 种实现。

- `ProducerCryptoFailureAction.FAIL`: Flink 程序将抛出异常并退出。
- `ProducerCryptoFailureAction.SEND`: 消息将以未加密的形态发送。

```java
PulsarCrypto pulsarCrypto = ...

PulsarSink<String> sink = PulsarSink.builder()
    ...
    .setPulsarCrypto(pulsarCrypto, ProducerCryptoFailureAction.FAIL)
    .build();
```

## 升级至最新的连接器

常见的升级步骤，请参阅[升级应用程序和 Flink 版本]({{< ref "docs/ops/upgrading" >}})。Pulsar 连接器没有在 Flink 端存储消费的状态，所有的消费信息都推送到了 Pulsar。所以需要注意下面的事项：

- 不要同时升级 Pulsar 连接器和 Pulsar 服务端的版本。
- 使用最新版本的 Pulsar 客户端来消费消息。

## 问题诊断

使用 Flink 和 Pulsar 交互时如果遇到问题，由于 Flink 内部实现只是基于 Pulsar 的 [Java 客户端](https://pulsar.apache.org/api/client/2.10.x/)和[管理 API](https://pulsar.apache.org/api/admin/2.10.x/) 而开发的。

用户遇到的问题可能与 Flink 无关，请先升级 Pulsar 的版本、Pulsar 客户端的版本，或者修改 Pulsar 的配置、Pulsar 连接器的配置来尝试解决问题。

## 已知问题

本节介绍有关 Pulsar 连接器的一些已知问题。

### 在 Java 11 上使用不稳定

Pulsar connector 在 Java 11 中有一些尚未修复的问题。我们当前推荐在 Java 8 环境中运行Pulsar connector.

### 不自动重连，而是抛出TransactionCoordinatorNotFound异常

Pulsar 事务机制仍在积极发展中，当前版本并不稳定。 Pulsar 2.9.2
引入了这个问题 [a break change](https://github.com/apache/pulsar/pull/13135)。
如果您使用 Pulsar 2.9.2或更高版本与较旧的 Pulsar 客户端一起使用，您可能会收到一个“TransactionCoordinatorNotFound”异常。

您可以使用最新的`pulsar-client-all`分支来解决这个问题。

{{< top >}}

[schema-evolution]: https://pulsar.apache.org/docs/2.10.x/schema-evolution-compatibility/#schema-evolution
[standard-metrics]: https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics
[tombstone-data-store]: https://en.wikipedia.org/wiki/Tombstone_(data_store)