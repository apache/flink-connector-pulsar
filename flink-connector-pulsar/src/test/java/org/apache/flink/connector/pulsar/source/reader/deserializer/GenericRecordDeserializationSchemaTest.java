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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.connector.pulsar.testutils.SampleData.Foo;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test of {@link GenericRecordDeserializationSchema}. */
class GenericRecordDeserializationSchemaTest extends PulsarTestSuiteBase {

    private final Random random = new Random(System.currentTimeMillis());

    @RegisterExtension
    private final MiniClusterExtension clusterExtension =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    @Test
    void deserializeMultipleAvroMessages() throws Exception {
        String topic = "generic-record-" + randomAlphanumeric(10);
        operator().createTopic(topic, 4);

        for (int i = 0; i < 100; i++) {
            Bar bar = new Bar(random.nextBoolean(), randomAlphanumeric(10));
            operator().sendMessage(topic, Schema.AVRO(Bar.class), bar);

            Foo foo = new Foo(random.nextInt(), random.nextFloat(), bar);
            operator().sendMessage(topic, Schema.AVRO(Foo.class), foo);
        }

        PulsarSource<String> source =
                PulsarSource.builder()
                        .setDeserializationSchema(new AvroGenericRecordDeserializer())
                        .setServiceUrl(operator().serviceUrl())
                        .setAdminUrl(operator().adminUrl())
                        .setTopics(topic)
                        .setStartCursor(StartCursor.earliest())
                        .setBoundedStopCursor(StopCursor.latest())
                        .setSubscriptionName("generic-record")
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromSource(source, noWatermarks(), "multiple-schema");
        List<String> results = stream.executeAndCollect(200);

        List<String> bars = results.stream().filter(s -> s.startsWith("{\"b\"")).collect(toList());
        List<String> foos =
                results.stream().filter(s -> s.startsWith("{\"bar\"")).collect(toList());

        assertThat(bars).hasSize(100);
        assertThat(foos).hasSize(100);

        for (String bar : bars) {
            Optional<String> exist = foos.stream().filter(f -> f.contains(bar)).findFirst();
            assertThat(exist).isPresent();
        }
    }

    @Override
    protected Map<String, String> runtimeConfigs() {
        return singletonMap("schemaCompatibilityStrategy", "ALWAYS_COMPATIBLE");
    }

    private static class AvroGenericRecordDeserializer
            implements GenericRecordDeserializer<String> {
        private static final long serialVersionUID = 3740038867903376007L;

        @Override
        public String deserialize(GenericRecord message) {
            assertEquals(SchemaType.AVRO, message.getSchemaType());
            Object object = message.getNativeObject();
            assertThat(object)
                    .isInstanceOf(
                            org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord.class);

            return object.toString();
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
