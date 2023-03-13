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

package org.apache.flink.connector.pulsar.table.catalog.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.PulsarConfiguration;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalog;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isSystemServiceNamespace;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.ALLOWED_SCHEMA_TYPES;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.NAMING_MAPPING_TOPIC;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_DEFAULT_PARTITION_SIZE;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_DEFAULT_SCHEMA_TYPE;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_HIDDEN_TENANT_PATTERN;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_MANAGED_TENANT;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.PULSAR_CATALOG_VISIBLE_TENANT_PATTERN;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.pulsar.common.naming.SystemTopicNames.isSystemTopic;
import static org.apache.pulsar.common.naming.TopicDomain.persistent;

/**
 * The configured class for the Pulsar catalog. It's required when user wants to manually create a
 * {@link PulsarCatalog}. You can use the following code to create this.
 *
 * <p>All the allowed config options for this class are defined in {@link PulsarCatalogOptions} and
 * {@link PulsarOptions}.
 *
 * <pre><code>
 * Configuration config = new Configuration();
 * config.set(PulsarOptions.PULSAR_SERVICE_URL, "pulsar://example.com:6650");
 * config.set(PulsarOptions.PULSAR_ADMIN_URL, "http://example.com:8080");
 *
 * CatalogConfiguration configuration = new CatalogConfiguration(config);
 * </code></pre>
 */
@PublicEvolving
public final class CatalogConfiguration extends PulsarConfiguration {
    private static final long serialVersionUID = -8289928767236331716L;

    private final String defaultDatabase;
    private final String managedTenant;
    @Nullable private final Pattern hiddenTenantPattern;
    private final Pattern visibleTenantPattern;
    private final SchemaType defaultSchemaType;
    private final int defaultPartitionSize;

    public CatalogConfiguration(Configuration configuration) {
        super(configuration);

        this.defaultDatabase = get(PULSAR_CATALOG_DEFAULT_DATABASE);
        this.managedTenant = get(PULSAR_CATALOG_MANAGED_TENANT);
        this.hiddenTenantPattern = get(PULSAR_CATALOG_HIDDEN_TENANT_PATTERN, Pattern::compile);
        this.visibleTenantPattern = get(PULSAR_CATALOG_VISIBLE_TENANT_PATTERN, Pattern::compile);
        this.defaultSchemaType = get(PULSAR_CATALOG_DEFAULT_SCHEMA_TYPE);
        this.defaultPartitionSize = get(PULSAR_CATALOG_DEFAULT_PARTITION_SIZE);

        // Add extra validation here for falling fast.
        checkArgument(
                ALLOWED_SCHEMA_TYPES.contains(defaultSchemaType),
                "We don't support such schema type: " + defaultSchemaType);
        checkArgument(defaultPartitionSize > 0, "The partition size should exceed 0");
    }

    /**
     * See the description in {@link PulsarCatalogOptions#PULSAR_CATALOG_DEFAULT_DATABASE} for
     * detailed information.
     */
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    /**
     * See the description in {@link PulsarCatalogOptions#PULSAR_CATALOG_MANAGED_TENANT} for
     * detailed information.
     */
    public String getManagedTenant() {
        return managedTenant;
    }

    /**
     * See the description in {@link PulsarCatalogOptions#PULSAR_CATALOG_DEFAULT_SCHEMA_TYPE} for
     * detailed information.
     */
    public SchemaType getDefaultSchemaType() {
        return defaultSchemaType;
    }

    /**
     * See the description in {@link PulsarCatalogOptions#PULSAR_CATALOG_DEFAULT_PARTITION_SIZE} for
     * detailed information.
     */
    public int getDefaultPartitionSize() {
        return defaultPartitionSize;
    }

    /**
     * Try to accomplish the database name. We will add {@link #managedTenant} for the Flink managed
     * database.
     */
    public String databaseName(String namespace) {
        if (namespace.contains("/")) {
            return namespace;
        } else {
            return NamespaceName.get(managedTenant, namespace).toString();
        }
    }

    /**
     * Filtering the system namespaces and all the internal tenants. This method will return true if
     * the given tenant is the managed tenant.
     *
     * @param namespace The value from {@link NamespaceName#toString()}
     */
    public boolean isInternalDatabase(String namespace) {
        // This should be a managed database.
        if (!namespace.contains("/")) {
            return false;
        }

        if (isSystemServiceNamespace(namespace)) {
            return true;
        }

        NamespaceName namespaceName = NamespaceName.get(namespace);
        String tenant = namespaceName.getTenant();

        return isInternalTenant(tenant);
    }

    public boolean isManagedTenant(String tenant) {
        return managedTenant.equals(tenant)
                // We also filter the default value of the managed tenant.
                || PULSAR_CATALOG_MANAGED_TENANT.defaultValue().equals(tenant);
    }

    public boolean isInternalTenant(String tenant) {
        if (isManagedTenant(tenant)) {
            return false;
        }

        if (hiddenTenantPattern != null && hiddenTenantPattern.matcher(tenant).matches()) {
            return true;
        }

        return !visibleTenantPattern.matcher(tenant).matches();
    }

    public boolean isInternalTable(String database, String table) {
        if (NAMING_MAPPING_TOPIC.equals(table)) {
            return true;
        }

        TopicName topicName = TopicName.get(persistent.value(), NamespaceName.get(database), table);
        return isSystemTopic(topicName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CatalogConfiguration that = (CatalogConfiguration) o;
        return Objects.equals(defaultDatabase, that.defaultDatabase)
                && Objects.equals(managedTenant, that.managedTenant)
                && Objects.equals(hiddenTenantPattern, that.hiddenTenantPattern)
                && Objects.equals(visibleTenantPattern, that.visibleTenantPattern)
                && Objects.equals(defaultSchemaType, that.defaultSchemaType)
                && Objects.equals(defaultPartitionSize, that.defaultPartitionSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                defaultDatabase,
                managedTenant,
                hiddenTenantPattern,
                visibleTenantPattern,
                defaultSchemaType,
                defaultPartitionSize);
    }
}
