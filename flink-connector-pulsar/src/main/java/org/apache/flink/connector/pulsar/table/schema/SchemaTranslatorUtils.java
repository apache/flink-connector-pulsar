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

package org.apache.flink.connector.pulsar.table.schema;

import org.apache.flink.api.common.typeutils.base.InstantSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalTimeSerializer;
import org.apache.flink.connector.pulsar.table.schema.translators.AvroSchemaTranslator;
import org.apache.flink.connector.pulsar.table.schema.translators.JSONSchemaTranslator;
import org.apache.flink.connector.pulsar.table.schema.translators.PrimitiveSchemaTranslator;
import org.apache.flink.connector.pulsar.table.schema.translators.ProtobufNativeSchemaTranslator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.EnumMap;
import java.util.Map;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.haveProtobuf;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Translator between the Flink schema and Pulsar schema. */
public final class SchemaTranslatorUtils {

    private static final Map<SchemaType, SchemaTranslator> TRANSLATOR_REGISTER =
            new EnumMap<>(SchemaType.class);

    static {
        registerSchemaFactory(new AvroSchemaTranslator());
        registerSchemaFactory(new JSONSchemaTranslator());
        if (haveProtobuf()) {
            registerSchemaFactory(new ProtobufNativeSchemaTranslator());
        }
        registerPrimitiveFactory(SchemaType.NONE, DataTypes.BYTES());
        registerPrimitiveFactory(SchemaType.BOOLEAN, DataTypes.BOOLEAN());
        registerPrimitiveFactory(SchemaType.INT8, DataTypes.TINYINT());
        registerPrimitiveFactory(SchemaType.INT16, DataTypes.SMALLINT());
        registerPrimitiveFactory(SchemaType.INT32, DataTypes.INT());
        registerPrimitiveFactory(SchemaType.INT64, DataTypes.BIGINT());
        registerPrimitiveFactory(SchemaType.FLOAT, DataTypes.FLOAT());
        registerPrimitiveFactory(SchemaType.DOUBLE, DataTypes.DOUBLE());
        registerPrimitiveFactory(SchemaType.BYTES, DataTypes.BYTES());
        registerPrimitiveFactory(SchemaType.STRING, DataTypes.STRING());
        registerPrimitiveFactory(SchemaType.TIMESTAMP, DataTypes.TIMESTAMP(3));
        registerPrimitiveFactory(SchemaType.TIME, DataTypes.TIME());
        registerPrimitiveFactory(SchemaType.DATE, DataTypes.DATE());
        registerPrimitiveFactory(
                SchemaType.INSTANT, DataTypes.RAW(Instant.class, InstantSerializer.INSTANCE));
        registerPrimitiveFactory(
                SchemaType.LOCAL_DATE,
                DataTypes.RAW(LocalDate.class, LocalDateSerializer.INSTANCE));
        registerPrimitiveFactory(
                SchemaType.LOCAL_TIME,
                DataTypes.RAW(LocalTime.class, LocalTimeSerializer.INSTANCE));
        registerPrimitiveFactory(
                SchemaType.LOCAL_DATE_TIME,
                DataTypes.RAW(LocalDateTime.class, LocalDateTimeSerializer.INSTANCE));
    }

    private SchemaTranslatorUtils() {
        // No public constructor
    }

    private static void registerPrimitiveFactory(SchemaType type, DataType dataType) {
        registerSchemaFactory(new PrimitiveSchemaTranslator(type, dataType));
    }

    private static void registerSchemaFactory(SchemaTranslator translator) {
        TRANSLATOR_REGISTER.put(translator.schemaType(), translator);
    }

    public static Schema convert(SchemaInfo info) {
        SchemaType type = info.getType();
        SchemaTranslator translator = TRANSLATOR_REGISTER.get(type);
        checkNotNull(translator, "We don't support this schema type: " + type);

        return translator.toSchema(info);
    }

    public static SchemaInfo convert(ResolvedSchema schema, SchemaType type) {
        SchemaTranslator translator = TRANSLATOR_REGISTER.get(type);
        checkNotNull(translator, "We don't support this schema type: " + type);

        return translator.toSchemaInfo(schema);
    }
}
