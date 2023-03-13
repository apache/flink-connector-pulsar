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

package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.common.schema.SchemaType;

import java.util.Set;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.CATALOG_CONFIG_PREFIX;
import static org.apache.flink.table.catalog.CommonCatalogOptions.DEFAULT_DATABASE_KEY;
import static org.apache.pulsar.common.schema.SchemaType.AVRO;
import static org.apache.pulsar.common.schema.SchemaType.JSON;

/**
 * Config options that is used to configure a {@link PulsarCatalog}. These config options are
 * specific to catalog only. Other runtime configurations can be found in {@link
 * org.apache.flink.connector.pulsar.common.config.PulsarOptions}.
 */
@PublicEvolving
@ConfigGroups(groups = {@ConfigGroup(name = "PulsarCatalog", keyPrefix = CATALOG_CONFIG_PREFIX)})
public final class PulsarCatalogOptions {

    // Pulsar catalog name.
    public static final String IDENTIFIER = "pulsar";
    public static final String LOCAL_IDENTIFIER = "local-pulsar";
    // Pulsar catalog config prefix.
    public static final String CATALOG_CONFIG_PREFIX = "pulsar.catalog.";
    // Default Pulsar connection endpoints.
    public static final String LOCAL_PULSAR_SERVICE_URL = "pulsar://127.0.0.1:6650";
    public static final String LOCAL_PULSAR_ADMIN_URL = "http://127.0.0.1:8080";
    // Managed topic for naming mapping.
    public static final String NAMING_MAPPING_TOPIC = "__flink_table_name_mapping";
    // Allowed schema types for tables.
    public static final Set<SchemaType> ALLOWED_SCHEMA_TYPES = ImmutableSet.of(JSON, AVRO);

    private PulsarCatalogOptions() {
        // This is a constant class
    }

    public static final ConfigOption<String> PULSAR_CATALOG_DEFAULT_DATABASE =
            ConfigOptions.key(CATALOG_CONFIG_PREFIX + "defaultDatabase")
                    .stringType()
                    .defaultValue("public/default")
                    .withFallbackKeys(DEFAULT_DATABASE_KEY)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This is the default database name to use when you enable the Pulsar Catalog.")
                                    .text(
                                            " We will use %s by default which will query all the tables under the % tenant and %s namespace.",
                                            code("public/default"), code("public"), code("default"))
                                    .text(
                                            " You can also config this option by using common table config name %s",
                                            code(DEFAULT_DATABASE_KEY))
                                    .build());

    public static final ConfigOption<String> PULSAR_CATALOG_MANAGED_TENANT =
            ConfigOptions.key(CATALOG_CONFIG_PREFIX + "managedTenant")
                    .stringType()
                    .defaultValue("__flink_catalog_managed")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If you create a database without the tenant information, we will create it under this tenant.")
                                    .text(
                                            " This tenant will be auto created when you enable the PulsarCatalog. Remember you admin token must has the authority to create it.")
                                    .text(
                                            " Or you can manually create a tenant in Pulsar for serving as the catalog managed tenant, and provide it here.")
                                    .build());

    public static final ConfigOption<String> PULSAR_CATALOG_HIDDEN_TENANT_PATTERN =
            ConfigOptions.key(CATALOG_CONFIG_PREFIX + "hiddenTenantPattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Different user may share the same Pulsar cluster with different managed tenants.")
                                    .text(
                                            " Use this regular expression to hide the tenants that you don't want to show to the users.")
                                    .build());

    public static final ConfigOption<String> PULSAR_CATALOG_VISIBLE_TENANT_PATTERN =
            ConfigOptions.key(CATALOG_CONFIG_PREFIX + "visibleTenantPattern")
                    .stringType()
                    .defaultValue(".*")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Use this regular expression to choose the tenants you want to use in catalog.")
                                    .text(
                                            " We will show all the tenants except the Pulsar's internal tenants by default.")
                                    .build());

    public static final ConfigOption<SchemaType> PULSAR_CATALOG_DEFAULT_SCHEMA_TYPE =
            ConfigOptions.key(CATALOG_CONFIG_PREFIX + "defaultSchemaType")
                    .enumType(SchemaType.class)
                    .defaultValue(JSON)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "We will create the schema for the tables created in catalog.")
                                    .text(" Use this option to defined the default schema type.")
                                    .text(
                                            " We only support %s and %s now.",
                                            code(JSON.name()), code(AVRO.name()))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_CATALOG_DEFAULT_PARTITION_SIZE =
            ConfigOptions.key(CATALOG_CONFIG_PREFIX + "defaultPartitionSize")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of the partition size for the tables created by catalog.")
                                    .text(" It should be a positive number.")
                                    .build());
}
