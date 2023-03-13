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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The translator for pulsar's primitive types. Currently, Pulsar supports <a
 * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">these primitive
 * types</a>.
 */
public class PrimitiveSchemaTranslator implements SchemaTranslator {

    public static final String SINGLE_FIELD_FIELD_NAME = "value";

    private final SchemaType schemaType;
    private final DataType dataType;

    public PrimitiveSchemaTranslator(SchemaType schemaType, DataType dataType) {
        checkArgument(schemaType.isPrimitive());

        this.schemaType = schemaType;
        this.dataType = dataType;
    }

    @Override
    public Schema toSchema(SchemaInfo info) {
        Field field = DataTypes.FIELD(SINGLE_FIELD_FIELD_NAME, dataType);
        return Schema.newBuilder().fromRowDataType(DataTypes.ROW(field)).build();
    }

    @Override
    public SchemaInfo toSchemaInfo(ResolvedSchema schema) {
        throw new UnsupportedOperationException(
                "Primitive Pulsar schema can't be generated from Flink schema.");
    }

    @Override
    public SchemaType schemaType() {
        return schemaType;
    }
}
