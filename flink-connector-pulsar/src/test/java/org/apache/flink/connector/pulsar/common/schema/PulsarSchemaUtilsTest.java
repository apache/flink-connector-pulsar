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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.connector.pulsar.SampleMessage.SubMessage;
import org.apache.flink.connector.pulsar.SampleMessage.SubMessage.NestedMessage;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.connector.pulsar.testutils.SampleData.FL;
import org.apache.flink.connector.pulsar.testutils.SampleData.Foo;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.isProtobufTypeClass;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarSchemaUtils}. */
class PulsarSchemaUtilsTest {

    @Test
    void haveProtobufShouldReturnTrueIfWeProvidedIt() {
        assertThat(PulsarSchemaUtils.haveProtobuf()).isTrue();
    }

    @Test
    void protobufClassValidation() {
        assertThat(isProtobufTypeClass(TestMessage.class)).isTrue();
        assertThat(isProtobufTypeClass(SubMessage.class)).isTrue();
        assertThat(isProtobufTypeClass(NestedMessage.class)).isTrue();

        assertThat(isProtobufTypeClass(Bar.class)).isFalse();
        assertThat(isProtobufTypeClass(FL.class)).isFalse();
        assertThat(isProtobufTypeClass(Foo.class)).isFalse();
    }

    @Test
    void createSchemaForComplexSchema() {
        // Avro
        Schema<Foo> avro1 = Schema.AVRO(Foo.class);
        PulsarSchema<Foo> avro2 = new PulsarSchema<>(avro1, Foo.class);
        SchemaInfo info1 = avro1.getSchemaInfo();
        assertThatThrownBy(() -> PulsarSchemaUtils.createSchema(info1))
                .isInstanceOf(NullPointerException.class);

        Schema<Foo> avro3 = PulsarSchemaUtils.createSchema(avro2.getSchemaInfo());
        assertThat(avro3.getSchemaInfo())
                .isNotEqualTo(avro1.getSchemaInfo())
                .isEqualTo(avro2.getSchemaInfo());

        // JSON
        Schema<FL> json1 = Schema.JSON(FL.class);
        PulsarSchema<FL> json2 = new PulsarSchema<>(json1, FL.class);
        Schema<FL> json3 = PulsarSchemaUtils.createSchema(json2.getSchemaInfo());

        assertThat(json3.getSchemaInfo())
                .isNotEqualTo(json1.getSchemaInfo())
                .isEqualTo(json2.getSchemaInfo());

        // Protobuf Native
        Schema<TestMessage> proto1 = Schema.PROTOBUF_NATIVE(TestMessage.class);
        PulsarSchema<TestMessage> proto2 = new PulsarSchema<>(proto1, TestMessage.class);
        Schema<TestMessage> proto3 = PulsarSchemaUtils.createSchema(proto2.getSchemaInfo());

        assertThat(proto3.getSchemaInfo())
                .isNotEqualTo(proto1.getSchemaInfo())
                .isEqualTo(proto2.getSchemaInfo());

        // KeyValue
        Schema<KeyValue<byte[], byte[]>> kvBytes1 = Schema.KV_BYTES();
        PulsarSchema<KeyValue<byte[], byte[]>> kvBytes2 =
                new PulsarSchema<>(kvBytes1, byte[].class, byte[].class);
        Schema<KeyValue<byte[], byte[]>> kvBytes3 =
                PulsarSchemaUtils.createSchema(kvBytes2.getSchemaInfo());

        assertThat(kvBytes3.getSchemaInfo()).isNotEqualTo(kvBytes1.getSchemaInfo());
    }

    @Test
    void encodeAndDecodeClassInfo() {
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        SchemaInfo info = schema.getSchemaInfo();
        SchemaInfo newInfo = PulsarSchemaUtils.encodeClassInfo(info, Foo.class);
        assertThatCode(() -> PulsarSchemaUtils.decodeClassInfo(newInfo)).doesNotThrowAnyException();

        Class<Foo> clazz = PulsarSchemaUtils.decodeClassInfo(newInfo);
        assertThat(clazz).isEqualTo(Foo.class);
    }
}
