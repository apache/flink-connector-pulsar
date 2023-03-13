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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Flink's {@link Schema} is used in table API while Pulsar's {@link SchemaInfo} is used for storing
 * the topic information. We use this translator for converting between these two schemas.
 */
@Internal
public interface SchemaTranslator {

    /** Convert the Pulsar's JSON schema definition into Flink's Schema instance. */
    Schema toSchema(SchemaInfo info);

    /**
     * Convert the resolved schema (the schema with type info) into a readable Pulsar schema
     * definition.
     */
    SchemaInfo toSchemaInfo(ResolvedSchema schema);

    /** The supported schema type for this translator. */
    SchemaType schemaType();
}
