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

package org.apache.flink.connector.pulsar.table.schema.translators;

import org.apache.flink.connector.pulsar.table.schema.SchemaTranslator;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;

import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.util.SchemaUtil;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.pulsar.table.schema.translators.PrimitiveSchemaTranslator.SINGLE_FIELD_FIELD_NAME;
import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.convertToSchema;

/** The translator for Pulsar's {@link AvroSchema}. */
public class AvroSchemaTranslator implements SchemaTranslator {

    @Override
    public Schema toSchema(SchemaInfo info) {
        String json = new String(info.getSchema(), StandardCharsets.UTF_8);
        DataType dataType = AvroSchemaConverter.convertToDataType(json);
        if (!(dataType instanceof FieldsDataType)) {
            // KeyValue type will be converted into the single value type.
            return Schema.newBuilder().column(SINGLE_FIELD_FIELD_NAME, dataType).build();
        }

        return Schema.newBuilder().fromRowDataType(dataType).build();
    }

    @Override
    public SchemaInfo toSchemaInfo(ResolvedSchema schema) {
        SchemaDefinition<?> definition =
                SchemaDefinition.builder()
                        .withJSR310ConversionEnabled(true)
                        .withAlwaysAllowNull(true)
                        .withJsonDef(schemaJsonDef(schema))
                        .withSupportSchemaVersioning(true)
                        .build();
        return SchemaUtil.parseSchemaInfo(definition, schemaType());
    }

    private String schemaJsonDef(ResolvedSchema schema) {
        List<org.apache.avro.Schema> fields = new ArrayList<>(schema.getColumnCount());
        for (Column column : schema.getColumns()) {
            org.apache.avro.Schema field =
                    convertToSchema(column.getDataType().getLogicalType(), column.getName());
            fields.add(field);
        }

        return org.apache.avro.Schema.createUnion(fields).toString(false);
    }

    @Override
    public SchemaType schemaType() {
        return SchemaType.AVRO;
    }
}
