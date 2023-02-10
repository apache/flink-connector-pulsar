package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.common.naming.TopicName;

import java.util.HashMap;
import java.util.Map;

/**
 * The internal implementation for supporting consuming the messages from Pulsar without a
 * predefined schema.
 */
@Internal
public class GenericRecordDeserializationSchema<T> implements PulsarDeserializationSchema<T> {
    private static final long serialVersionUID = 1133225716807307498L;

    private transient PulsarClientImpl client;
    private transient Map<String, AutoConsumeSchema> schemaMap;

    private final GenericRecordDeserializer<T> deserializer;

    public GenericRecordDeserializationSchema(GenericRecordDeserializer<T> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<T> out) throws Exception {
        AutoConsumeSchema schema = getSchema(message);
        GenericRecord element = schema.decode(message.getData(), message.getSchemaVersion());
        T msg = deserializer.deserialize(element);

        out.collect(msg);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void open(PulsarInitializationContext context, SourceConfiguration configuration)
            throws Exception {
        this.client = (PulsarClientImpl) context.getPulsarClient();
        this.schemaMap = new HashMap<>();
    }

    /** Get or create an auto schema instance for this topic partition. */
    private AutoConsumeSchema getSchema(Message<byte[]> message) {
        String topic = message.getTopicName();
        AutoConsumeSchema schema = schemaMap.get(topic);
        if (schema != null) {
            return schema;
        }

        schema = new AutoConsumeSchema();

        // Set dynamic schema info provider.
        TopicName topicName = TopicName.get(topic);
        MultiVersionSchemaInfoProvider provider =
                new MultiVersionSchemaInfoProvider(topicName, client);
        schema.setSchemaInfoProvider(provider);

        schemaMap.put(topic, schema);
        return schema;
    }
}
