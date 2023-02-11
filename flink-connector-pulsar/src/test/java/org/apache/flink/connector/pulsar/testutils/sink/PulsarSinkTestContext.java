package org.apache.flink.connector.pulsar.testutils.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.sink.reader.PulsarPartitionDataReader;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.api.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode.ROUND_ROBIN;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.toDeliveryGuarantee;

/**
 * Common sink test context for the pulsar based tests. We use the string text as the basic send
 * content.
 */
public abstract class PulsarSinkTestContext extends PulsarTestContext<String>
        implements DataStreamSinkV2ExternalContext<String> {

    private static final String TOPIC_NAME_PREFIX = "persistent://public/default/flink-sink-topic-";
    private static final int RECORD_SIZE_UPPER_BOUND = 300;
    private static final int RECORD_SIZE_LOWER_BOUND = 100;
    private static final int RECORD_STRING_SIZE = 20;

    private List<String> topics = generateTopics();
    private final Closer closer = Closer.create();

    public PulsarSinkTestContext(PulsarTestEnvironment environment) {
        super(environment, Schema.STRING);
    }

    @Override
    public Sink<String> createSink(TestingSinkSettings sinkSettings) {
        // Create the topic if it needs.
        if (creatTopic()) {
            for (String topic : topics) {
                try {
                    operator.createTopic(topic, 4);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }

        // We don't have NONE delivery guarantee and no need to test it by default.
        DeliveryGuarantee guarantee = toDeliveryGuarantee(sinkSettings.getCheckpointingMode());

        PulsarSinkBuilder<String> builder =
                PulsarSink.builder()
                        .setServiceUrl(operator.serviceUrl())
                        .setAdminUrl(operator.adminUrl())
                        .setDeliveryGuarantee(guarantee)
                        .setSerializationSchema(schema)
                        .enableSchemaEvolution()
                        .setConfig(PULSAR_BATCHING_MAX_MESSAGES, 4);
        if (creatTopic()) {
            builder.setTopics(topics).setTopicRoutingMode(ROUND_ROBIN);
        } else {
            builder.setTopicRouter(new CustomTopicRouter(topics));
        }

        setSinkBuilder(builder);

        return builder.build();
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        PulsarPartitionDataReader<String> reader = createSinkDataReader(topics);
        closer.register(reader);

        return reader;
    }

    @Override
    public List<String> generateTestData(TestingSinkSettings sinkSettings, long seed) {
        Random random = new Random(seed);
        int recordSize =
                random.nextInt(RECORD_SIZE_UPPER_BOUND - RECORD_SIZE_LOWER_BOUND)
                        + RECORD_SIZE_LOWER_BOUND;
        List<String> records = new ArrayList<>(recordSize);
        for (int i = 0; i < recordSize; i++) {
            int size = random.nextInt(RECORD_STRING_SIZE) + RECORD_STRING_SIZE;
            String record = "index:" + i + "-data:" + randomAlphanumeric(size);
            records.add(record);
        }

        return records;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }

    @Override
    public void close() throws Exception {
        // Switch to another topic info after finishing a test case.
        this.topics = generateTopics();

        closer.close();
    }

    /** Return topic info. We would use only one topic by default. */
    protected List<String> generateTopics() {
        String topicName = TOPIC_NAME_PREFIX + randomAlphanumeric(8);
        return singletonList(topicName);
    }

    /** Override the default sink behavior. */
    protected void setSinkBuilder(PulsarSinkBuilder<String> builder) {
        // Nothing to do by default.
    }

    /** Set it to false for testing topic auto creation feature. */
    protected boolean creatTopic() {
        return true;
    }

    protected PulsarPartitionDataReader<String> createSinkDataReader(List<String> topics) {
        return new PulsarPartitionDataReader<>(operator, topics, Schema.STRING);
    }

    /**
     * Custom topic router for auto-generated topics. We will switch to another topic every 10
     * messages.
     */
    public static class CustomTopicRouter implements TopicRouter<String> {
        private static final long serialVersionUID = 1698701183626468094L;

        private final List<String> topics;
        private final AtomicInteger counter;

        public CustomTopicRouter(List<String> topics) {
            this.topics = topics;
            this.counter = new AtomicInteger(0);
        }

        @Override
        public TopicPartition route(
                String s, String key, List<TopicPartition> partitions, PulsarSinkContext context) {
            int index = counter.incrementAndGet() / 10 % topics.size();
            String topic = topics.get(index);
            return new TopicPartition(topic);
        }
    }
}
