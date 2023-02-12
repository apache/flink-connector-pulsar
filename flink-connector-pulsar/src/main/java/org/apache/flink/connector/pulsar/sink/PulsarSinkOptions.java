/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.writer.router.MessageKeyHash;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

import java.time.Duration;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PRODUCER_CONFIG_PREFIX;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.SINK_CONFIG_PREFIX;
import static org.apache.flink.connector.pulsar.sink.writer.router.MessageKeyHash.MURMUR3_32_HASH;
import static org.apache.pulsar.client.impl.conf.ProducerConfigurationData.DEFAULT_BATCHING_MAX_MESSAGES;

/**
 * Configurations for PulsarSink. All the options list here could be configured in {@link
 * PulsarSinkBuilder#setConfig(ConfigOption, Object)}. The {@link PulsarOptions} is also required
 * for pulsar source.
 *
 * @see PulsarOptions for shared configure options.
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(name = "PulsarSink", keyPrefix = SINK_CONFIG_PREFIX),
            @ConfigGroup(name = "PulsarProducer", keyPrefix = PRODUCER_CONFIG_PREFIX)
        })
public final class PulsarSinkOptions {

    // Pulsar sink connector config prefix.
    public static final String SINK_CONFIG_PREFIX = "pulsar.sink.";
    // Pulsar producer API config prefix.
    public static final String PRODUCER_CONFIG_PREFIX = "pulsar.producer.";

    private PulsarSinkOptions() {
        // This is a constant class
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for pulsar sink part.
    // All the configuration listed below should have the pulsar.sink prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<DeliveryGuarantee> PULSAR_WRITE_DELIVERY_GUARANTEE =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "deliveryGuarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.NONE)
                    .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<Long> PULSAR_WRITE_TRANSACTION_TIMEOUT =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "transactionTimeoutMillis")
                    .longType()
                    .defaultValue(Duration.ofHours(3).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This option is used when the user require the %s semantic. ",
                                            code("DeliveryGuarantee.EXACTLY_ONCE"))
                                    .text(
                                            "We would use transaction for making sure the message could be write only once.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_TOPIC_METADATA_REFRESH_INTERVAL =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "topicMetadataRefreshInterval")
                    .longType()
                    .defaultValue(Duration.ofMinutes(30).toMillis())
                    .withDescription(
                            "Auto update the topic metadata in a fixed interval (in ms). The default value is 30 minutes.");

    public static final ConfigOption<MessageKeyHash> PULSAR_MESSAGE_KEY_HASH =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "messageKeyHash")
                    .enumType(MessageKeyHash.class)
                    .defaultValue(MURMUR3_32_HASH)
                    .withDescription(
                            "The hash policy for routing message by calculating the hash code of message key.");

    public static final ConfigOption<Boolean> PULSAR_WRITE_SCHEMA_EVOLUTION =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "enableSchemaEvolution")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If you enable this option and use %s,"
                                                    + " we would produce and serialize the message by using Pulsar's %s.",
                                            code(
                                                    "PulsarSinkBuilder.setSerializationSchema(Schema)"),
                                            code("Schema"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_RECOMMIT_TIMES =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "maxRecommitTimes")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The allowed transaction recommit times if we meet some retryable exception."
                                    + " This is used in Pulsar Transaction.");

    public static final ConfigOption<Boolean> PULSAR_ENABLE_SINK_METRICS =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "enableMetrics")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The metrics from Pulsar Producer are only exposed if you enable this option. ")
                                    .text(
                                            "You should set the %s to a positive value if you enable this option.",
                                            code(PULSAR_STATS_INTERVAL_SECONDS.key()))
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_VALIDATE_SINK_MESSAGE_BYTES =
            ConfigOptions.key(SINK_CONFIG_PREFIX + "validateSinkMessageBytes")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Pulsar client can validate the raw message bytes with the latest topic schema. ")
                                    .text(
                                            "This can make sure your serialized messages bytes is valid for consumer.")
                                    .build());

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ProducerConfigurationData part.
    // All the configuration listed below should have the pulsar.producer prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_PRODUCER_NAME =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "producerName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A producer name which would be displayed in the Pulsar's dashboard."
                                    + " If no producer name was provided, we would use a Pulsar generated name instead.");

    public static final ConfigOption<Long> PULSAR_SEND_TIMEOUT_MS =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "sendTimeoutMs")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            Description.builder()
                                    .text("Message send timeout in ms.")
                                    .text(
                                            "If a message is not acknowledged by a server before the %s expires, an error occurs.",
                                            code("sendTimeout"))
                                    .build());

    public static final ConfigOption<Long> PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingMaxPublishDelayMicros")
                    .longType()
                    .defaultValue(MILLISECONDS.toMicros(1))
                    .withDescription("Batching time period of sending messages.");

    public static final ConfigOption<Integer>
            PULSAR_BATCHING_PARTITION_SWITCH_FREQUENCY_BY_PUBLISH_DELAY =
                    ConfigOptions.key(
                                    PRODUCER_CONFIG_PREFIX
                                            + "batchingPartitionSwitchFrequencyByPublishDelay")
                            .intType()
                            .defaultValue(10)
                            .withDescription(
                                    "The maximum wait time for switching topic partitions.");

    public static final ConfigOption<Integer> PULSAR_BATCHING_MAX_MESSAGES =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingMaxMessages")
                    .intType()
                    .defaultValue(DEFAULT_BATCHING_MAX_MESSAGES)
                    .withDescription("The maximum number of messages permitted in a batch.");

    public static final ConfigOption<Integer> PULSAR_BATCHING_MAX_BYTES =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingMaxBytes")
                    .intType()
                    .defaultValue(128 * 1024)
                    .withDescription(
                            "The maximum size of messages permitted in a batch. Keep the maximum consistent as previous versions.");

    public static final ConfigOption<Boolean> PULSAR_BATCHING_ENABLED =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "batchingEnabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable batch send ability, it was enabled by default.");

    public static final ConfigOption<Boolean> PULSAR_CHUNKING_ENABLED =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "chunkingEnabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If message size is higher than allowed max publish-payload size by broker,")
                                    .text(
                                            " then enableChunking helps producer to split message into multiple chunks")
                                    .text(" and publish them to broker separately and in order.")
                                    .text(
                                            " So, it allows client to successfully publish large size of messages in pulsar.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_CHUNK_MAX_MESSAGE_SIZE =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "chunkMaxMessageSize")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            Description.builder()
                                    .text("Max chunk message size in bytes.")
                                    .text(
                                            " Producer chunks the message if chunking is enabled and message size is larger than max chunk-message size.")
                                    .text(
                                            " By default, chunkMaxMessageSize value is -1 and producer chunks based on max-message size configured at the broker.")
                                    .build());

    public static final ConfigOption<CompressionType> PULSAR_COMPRESSION_TYPE =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "compressionType")
                    .enumType(CompressionType.class)
                    .defaultValue(CompressionType.NONE)
                    .withDescription(
                            Description.builder()
                                    .text("Message data compression type used by a producer.")
                                    .text("Available options:")
                                    .list(
                                            link("LZ4", "https://github.com/lz4/lz4"),
                                            link("ZLIB", "https://zlib.net/"),
                                            link("ZSTD", "https://facebook.github.io/zstd/"),
                                            link("SNAPPY", "https://google.github.io/snappy/"))
                                    .build());

    public static final ConfigOption<Long> PULSAR_INITIAL_SEQUENCE_ID =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "initialSequenceId")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "The sequence id for avoiding the duplication, it's used when Pulsar doesn't have transaction.");

    public static final ConfigOption<Map<String, String>> PULSAR_PRODUCER_PROPERTIES =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "properties")
                    .mapType()
                    .defaultValue(emptyMap())
                    .withDescription(
                            Description.builder()
                                    .text("A name or value property of this consumer.")
                                    .text(
                                            " %s is application defined metadata attached to a consumer.",
                                            code("properties"))
                                    .text(
                                            " When getting a topic stats, associate this metadata with the consumer stats for easier identification.")
                                    .build());

    public static final ConfigOption<ProducerCryptoFailureAction>
            PULSAR_PRODUCER_CRYPTO_FAILURE_ACTION =
                    ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "producerCryptoFailureAction")
                            .enumType(ProducerCryptoFailureAction.class)
                            .defaultValue(ProducerCryptoFailureAction.FAIL)
                            .withDescription(
                                    "The action the producer will take in case of encryption failures.");
}
