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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.table.sink.PulsarTableSerializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.sink.PulsarTableSink;
import org.apache.flink.connector.pulsar.table.source.PulsarTableDeserializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.source.PulsarTableSource;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.createKeyFormatProjection;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.createValueFormatProjection;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getKeyDecodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getKeyEncodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getMessageDelayMillis;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getPulsarProperties;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getStartCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getStopCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getSubscriptionType;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicListFromOptions;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicRouter;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueDecodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getValueEncodingFormat;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.EXPLICIT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_MESSAGE_DELAY_INTERVAL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AFTER_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.VALUE_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateTableSinkOptions;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateTableSourceOptions;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateUpsertModeKeyConstraints;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.pulsar.shade.org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * Factory for creating {@link DynamicTableSource} and {@link DynamicTableSink} for upsert-pulsar.
 *
 * <p>The major difference between this factory and {@link PulsarTableFactory} is the primary key
 * options are overridden in this factory.
 */
public class UpsertPulsarTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "upsert-pulsar";

    public static final String DEFAULT_SUBSCRIPTION_NAME_PREFIX = "flink-upsert-pulsar-";

    public static final boolean UPSERT_ENABLED = true;

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // Format options should be retrieved before validation.
        final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
                getKeyDecodingFormat(helper);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);
        ReadableConfig tableOptions = helper.getOptions();

        // Validate configs are not conflict; each options is consumed; no unwanted configs
        // PulsarOptions, PulsarSourceOptions and PulsarSinkOptions is not part of the validation.
        helper.validateExcept(
                PulsarOptions.CLIENT_CONFIG_PREFIX,
                PulsarSourceOptions.SOURCE_CONFIG_PREFIX,
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX,
                PulsarSinkOptions.SINK_CONFIG_PREFIX);

        // Retrieve configs
        final List<String> topics = getTopicListFromOptions(tableOptions);
        final StartCursor startCursor = getStartCursor(tableOptions);
        final StopCursor stopCursor = getStopCursor(tableOptions);
        final SubscriptionType subscriptionType = getSubscriptionType(tableOptions);

        // Forward source configs
        final Properties properties = getPulsarProperties(tableOptions);
        properties.setProperty(PULSAR_SERVICE_URL.key(), tableOptions.get(SERVICE_URL));
        // Set random subscriptionName if not provided
        properties.setProperty(
                PULSAR_SUBSCRIPTION_NAME.key(),
                tableOptions
                        .getOptional(SOURCE_SUBSCRIPTION_NAME)
                        .orElse(DEFAULT_SUBSCRIPTION_NAME_PREFIX + randomAlphabetic(5)));
        // Retrieve physical fields (not including computed or metadata fields),
        // and projections and create a schema factory based on such information.
        final DataType physicalDataType = context.getPhysicalRowDataType();

        overrideKeyFieldOptionsForUpsertMode((Configuration) tableOptions, context);
        validateUpsertModeKeyConstraints(tableOptions, context.getPrimaryKeyIndexes());
        validateTableSourceOptions(tableOptions);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);
        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarTableDeserializationSchemaFactory(
                        physicalDataType,
                        keyDecodingFormat,
                        keyProjection,
                        valueDecodingFormat,
                        valueProjection,
                        UPSERT_ENABLED);

        // Set default values for configuration not exposed to user.
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormatForMetadataPushdown =
                valueDecodingFormat;
        final ChangelogMode changelogMode =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();

        return new PulsarTableSource(
                deserializationSchemaFactory,
                decodingFormatForMetadataPushdown,
                changelogMode,
                topics,
                properties,
                startCursor,
                stopCursor,
                subscriptionType);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // Format options should be retrieved before validation.
        final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
                getKeyEncodingFormat(helper);
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                getValueEncodingFormat(helper);
        ReadableConfig tableOptions = helper.getOptions();

        // Validate configs are not conflict; each options is consumed; no unwanted configs
        // PulsarOptions, PulsarSourceOptions and PulsarSinkOptions is not part of the validation.
        helper.validateExcept(
                PulsarOptions.CLIENT_CONFIG_PREFIX,
                PulsarSourceOptions.SOURCE_CONFIG_PREFIX,
                PulsarSourceOptions.CONSUMER_CONFIG_PREFIX,
                PulsarSinkOptions.PRODUCER_CONFIG_PREFIX,
                PulsarSinkOptions.SINK_CONFIG_PREFIX);

        // Retrieve configs
        final TopicRouter<RowData> topicRouter =
                getTopicRouter(tableOptions, context.getClassLoader());

        final long messageDelayMillis = getMessageDelayMillis(tableOptions);
        final List<String> topics = getTopicListFromOptions(tableOptions);

        // Forward sink configs
        final Properties properties = getPulsarProperties(tableOptions);
        properties.setProperty(PULSAR_SERVICE_URL.key(), tableOptions.get(SERVICE_URL));

        // Retrieve physical DataType (not including computed or metadata fields)
        final DataType physicalDataType = context.getPhysicalRowDataType();

        // retrieve primary key and replace key.fields
        overrideKeyFieldOptionsForUpsertMode((Configuration) tableOptions, context);
        validateUpsertModeKeyConstraints(tableOptions, context.getPrimaryKeyIndexes());
        validateTableSinkOptions(tableOptions);

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);
        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final PulsarTableSerializationSchemaFactory serializationSchemaFactory =
                new PulsarTableSerializationSchemaFactory(
                        physicalDataType,
                        keyEncodingFormat,
                        keyProjection,
                        valueEncodingFormat,
                        valueProjection,
                        UPSERT_ENABLED);

        // Set default values for configuration not exposed to user.
        final DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        final ChangelogMode changelogMode =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.UPDATE_AFTER)
                        .addContainedKind(RowKind.DELETE)
                        .build();
        // upsert mode only supports message_key_hash
        final TopicRoutingMode topicRoutingMode = TopicRoutingMode.MESSAGE_KEY_HASH;

        return new PulsarTableSink(
                serializationSchemaFactory,
                changelogMode,
                topics,
                properties,
                deliveryGuarantee,
                topicRouter,
                topicRoutingMode,
                messageDelayMillis);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(TOPICS, SERVICE_URL).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        FactoryUtil.FORMAT,
                        VALUE_FORMAT,
                        SOURCE_SUBSCRIPTION_NAME,
                        SOURCE_START_FROM_MESSAGE_ID,
                        SOURCE_START_FROM_PUBLISH_TIME,
                        SOURCE_STOP_AT_MESSAGE_ID,
                        SOURCE_STOP_AFTER_MESSAGE_ID,
                        SOURCE_STOP_AT_PUBLISH_TIME,
                        SINK_MESSAGE_DELAY_INTERVAL,
                        SINK_PARALLELISM,
                        KEY_FORMAT,
                        EXPLICIT)
                .collect(Collectors.toSet());
    }

    /**
     * Format and Delivery guarantee related options are not forward options.
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        TOPICS,
                        SERVICE_URL,
                        SOURCE_SUBSCRIPTION_NAME,
                        SOURCE_START_FROM_MESSAGE_ID,
                        SOURCE_START_FROM_PUBLISH_TIME,
                        SOURCE_STOP_AT_MESSAGE_ID,
                        SOURCE_STOP_AFTER_MESSAGE_ID,
                        SOURCE_STOP_AT_PUBLISH_TIME,
                        SINK_MESSAGE_DELAY_INTERVAL)
                .collect(Collectors.toSet());
    }

    private void overrideKeyFieldOptionsForUpsertMode(Configuration tableOptions, Context context) {
        // override key fields
        List<String> keyFields =
                context.getCatalogTable().getResolvedSchema().getPrimaryKey().get().getColumns();
        tableOptions.set(KEY_FIELDS, keyFields);
    }
}
