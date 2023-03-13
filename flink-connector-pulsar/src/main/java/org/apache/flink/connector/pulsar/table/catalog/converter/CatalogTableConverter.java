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

package org.apache.flink.connector.pulsar.table.catalog.converter;

import org.apache.flink.connector.pulsar.table.catalog.config.CatalogConfiguration;
import org.apache.flink.connector.pulsar.table.schema.SchemaTranslatorUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.flink.shaded.guava30.com.google.common.base.Converter;

import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A converter for converting the {@link CatalogBaseTable} to Pulsar topic schema's properties and
 * backward converting.
 *
 * <p>We will support two types of conversion in this tool. They are the tables created by catalog
 * and the tables that are not created and managed by catalog. We will use an internal flag {@code
 * FLINK_TABLE_FLAG} in schema properties to mark the related tables as the catalog table.
 *
 * <p>All the tables created by the flink catalog will hava a translated schema which matches the
 * given table columns.
 */
@SuppressWarnings("java:S2160")
public class CatalogTableConverter extends Converter<CatalogBaseTable, SchemaInfo> {

    // An internal flag for marking all the Flink catalog created topics.
    // We can perform the modification only on these topics.
    private static final String FLINK_TABLE_FLAG = "__flink_managed_table";

    private final CatalogConfiguration configuration;

    public CatalogTableConverter(CatalogConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected SchemaInfo doForward(CatalogBaseTable baseTable) {
        checkArgument(baseTable instanceof ResolvedCatalogTable);
        ResolvedCatalogTable table = (ResolvedCatalogTable) baseTable;

        Map<String, String> properties = table.toProperties();
        properties.put(FLINK_TABLE_FLAG, "true");

        // Generate the schema. We will support setting the schema type in the extra options.
        ResolvedSchema schema = table.getResolvedSchema();
        SchemaType schemaType = configuration.getDefaultSchemaType();

        return SchemaTranslatorUtils.convert(schema, schemaType);
    }

    @Override
    protected CatalogBaseTable doBackward(SchemaInfo info) {
        if (isFlinkTable(info)) {
            return CatalogTable.fromProperties(info.getProperties());
        }

        // This is a Pulsar native table, we will try to convert it into a Flink table.
        Schema schema = SchemaTranslatorUtils.convert(info);
        return CatalogTable.of(schema, "", emptyList(), emptyMap());
    }

    public boolean isFlinkTable(SchemaInfo info) {
        Map<String, String> properties = info.getProperties();
        return properties.containsKey(FLINK_TABLE_FLAG);
    }
}
