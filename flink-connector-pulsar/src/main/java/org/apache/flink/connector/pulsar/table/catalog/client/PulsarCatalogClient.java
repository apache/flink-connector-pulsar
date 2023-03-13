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

package org.apache.flink.connector.pulsar.table.catalog.client;

import org.apache.flink.connector.pulsar.table.catalog.config.CatalogConfiguration;
import org.apache.flink.connector.pulsar.table.catalog.converter.CatalogDatabaseConverter;
import org.apache.flink.connector.pulsar.table.catalog.converter.CatalogTableConverter;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.Mode;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.isSystemServiceNamespace;
import static org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE;

/**
 * A catalog client for connecting to Pulsar with admin API. This client also provides all the
 * operations for the Pulsar catalog.
 */
public class PulsarCatalogClient implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCatalogClient.class);

    private final String catalogName;
    private final PulsarAdmin admin;
    private final TableNameMapper nameMapper;
    private final String managedTenant;
    private final CatalogConfiguration configuration;

    // The converters for simplifying the transform logics.
    private final CatalogDatabaseConverter databaseConverter;
    private final CatalogTableConverter tableConverter;

    public PulsarCatalogClient(String catalogName, CatalogConfiguration configuration)
            throws PulsarClientException {
        this.catalogName = catalogName;
        this.admin = createAdmin(configuration);
        this.nameMapper = new TableNameMapper(admin);
        this.managedTenant = configuration.getManagedTenant();
        this.configuration = configuration;

        this.databaseConverter = new CatalogDatabaseConverter();
        this.tableConverter = new CatalogTableConverter(configuration);
    }

    public void initializeManagedTenant() throws PulsarAdminException {
        try {
            TenantInfo info = admin.tenants().getTenantInfo(managedTenant);
            LOG.debug("The tenant {} has been created with info {}", managedTenant, info);
        } catch (NotFoundException e) {
            // Create the managed tenant.
            LOG.debug("Don't have the managed tenant {}, try to create it.", managedTenant);
            List<String> clusters = admin.clusters().getClusters();
            TenantInfo tenantInfo =
                    TenantInfo.builder().allowedClusters(new HashSet<>(clusters)).build();
            admin.tenants().createTenant(managedTenant, tenantInfo);
        }
    }

    public List<String> listDatabases() throws PulsarAdminException {
        List<String> result = new ArrayList<>();

        // List all the Pulsar databases.
        List<String> tenants = admin.tenants().getTenants();
        for (String tenant : tenants) {
            if (configuration.isManagedTenant(tenant) || configuration.isInternalTenant(tenant)) {
                continue;
            }
            List<String> namespaces = admin.namespaces().getNamespaces(tenant);
            for (String namespace : namespaces) {
                if (!isSystemServiceNamespace(namespace)) {
                    result.add(namespace);
                }
            }
        }

        // List all the Flink managed databases.
        List<String> namespaces = admin.namespaces().getNamespaces(managedTenant);
        for (String namespace : namespaces) {
            // Use this parsing method instead of directly split it.
            result.add(NamespaceName.get(namespace).getLocalName());
        }

        if (result.isEmpty()) {
            LOG.warn(
                    "No databases are allowed to show to the end user. Check you config: {}",
                    configuration);
        }

        return result;
    }

    public CatalogDatabase getDatabase(String name)
            throws DatabaseNotExistException, PulsarAdminException {
        if (configuration.isInternalDatabase(name)) {
            IllegalArgumentException e =
                    new IllegalArgumentException("You can't access the internal database");
            throw new DatabaseNotExistException(catalogName, name, e);
        }

        try {
            String databaseName = configuration.databaseName(name);
            Map<String, String> properties = admin.namespaces().getProperties(databaseName);

            return databaseConverter.reverse().convert(properties);
        } catch (NotFoundException e) {
            throw new DatabaseNotExistException(catalogName, name, e);
        }
    }

    public boolean databaseExists(String name) throws PulsarAdminException {
        if (configuration.isInternalDatabase(name)) {
            LOG.debug("This is an internal database {}, return false for existing checking.", name);
            return false;
        }

        try {
            String databaseName = configuration.databaseName(name);
            admin.namespaces().getProperties(databaseName);

            // No exception has been thrown means this is an existed database.
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws PulsarAdminException, DatabaseAlreadyExistException {
        if (configuration.isInternalDatabase(name)) {
            throw new CatalogException(name + " is a preserved Pulsar internal database");
        }

        String databaseName = configuration.databaseName(name);

        try {
            admin.namespaces().createNamespace(databaseName);
        } catch (ConflictException e) {
            // The database is already existed in Pulsar.
            if (ignoreIfExists) {
                // Skip the properties setting for an existed database.
                return;
            } else {
                throw new DatabaseAlreadyExistException(catalogName, name, e);
            }
        }

        Map<String, String> properties = databaseConverter.convert(database);
        admin.namespaces().setProperties(databaseName, properties);
    }

    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws PulsarAdminException, DatabaseNotExistException, DatabaseNotEmptyException {
        if (configuration.isInternalDatabase(name)) {
            throw new CatalogException(
                    name + " is an internal database, you don't have write access");
        }

        String databaseName = configuration.databaseName(name);

        // List all the topics include the non-persistent topics for checking.
        List<String> topics;
        try {
            ListNamespaceTopicsOptions options =
                    ListNamespaceTopicsOptions.builder()
                            .includeSystemTopic(false)
                            .mode(Mode.ALL)
                            .build();
            topics = admin.namespaces().getTopics(databaseName, options);
        } catch (NotFoundException e) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(catalogName, name, e);
            }
        }

        // Check if this is an empty database.
        if (!topics.isEmpty() && !cascade) {
            throw new DatabaseNotEmptyException(catalogName, name);
        }

        // Delete all the tables under this database.
        for (String topic : topics) {
            LOG.warn("Delete table {} due to we want to remove the database {}", topic, name);
            admin.topics().delete(topic, true);
        }

        // Delete the database itself with the force flag to remove all the tables.
        admin.namespaces().deleteNamespace(databaseName, true);
    }

    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws PulsarAdminException, DatabaseNotExistException {
        if (configuration.isInternalDatabase(name)) {
            throw new CatalogException(
                    name + " is an internal database, you don't have write access");
        }

        String databaseName = configuration.databaseName(name);

        try {
            admin.namespaces().getProperties(databaseName);
        } catch (NotFoundException e) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(catalogName, name, e);
            }
        }

        // Update the database properties.
        Map<String, String> properties = databaseConverter.convert(newDatabase);
        admin.namespaces().setProperties(databaseName, properties);
    }

    public List<String> listTables(String name)
            throws PulsarAdminException, DatabaseNotExistException {
        if (configuration.isInternalDatabase(name)) {
            throw new CatalogException(name + " is an internal database, you can't get its tables");
        }

        String databaseName = configuration.databaseName(name);

        // We only support the persistent topics.
        ListNamespaceTopicsOptions options =
                ListNamespaceTopicsOptions.builder()
                        .includeSystemTopic(false)
                        .mode(Mode.PERSISTENT)
                        .build();
        try {
            List<String> topics = admin.namespaces().getTopics(databaseName, options);
            Map<String, String> nameMapping =
                    nameMapper.listMapping(databaseName).entrySet().stream()
                            .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));

            // Convert the topic name into table name.
            List<String> tables = new ArrayList<>(topics.size());
            for (String topic : topics) {
                String table = TopicName.get(topic).getLocalName();
                if (!configuration.isInternalTable(databaseName, table)) {
                    tables.add(nameMapping.getOrDefault(table, table));
                }
            }

            return tables;
        } catch (NotFoundException e) {
            throw new DatabaseNotExistException(catalogName, name, e);
        }
    }

    public boolean tableExists(ObjectPath tablePath) throws PulsarAdminException {
        String databaseName = configuration.databaseName(tablePath.getDatabaseName());
        String tableName = nameMapper.getMapping(databaseName, tablePath.getObjectName());
        try {
            String table = TopicName.get(tableName).getLocalName();
            if (configuration.isInternalTable(databaseName, table)) {
                return false;
            }

            admin.topics().getPartitionedTopicMetadata(tableName);
            return true;
        } catch (NotFoundException e) {
            return false;
        }
    }

    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws PulsarAdminException, TableNotExistException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(catalogName, tablePath);
            }
        }

        String databaseName = configuration.databaseName(tablePath.getDatabaseName());
        String tableName = nameMapper.dropMapping(databaseName, tablePath.getObjectName());
        String table = TopicName.get(tableName).getLocalName();
        if (configuration.isInternalTable(databaseName, table)) {
            throw new CatalogException(
                    "This is a internal table " + tablePath.getObjectName() + " in Pulsar catalog");
        }

        admin.topics().delete(tableName, true);
        admin.schemas().deleteSchema(tableName);
    }

    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws PulsarAdminException, TableNotExistException, TableAlreadyExistException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            } else {
                return;
            }
        }

        ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        if (tableExists(newPath)) {
            throw new TableAlreadyExistException(catalogName, newPath);
        }

        String databaseName = configuration.databaseName(newPath.getDatabaseName());
        nameMapper.addMapping(databaseName, tablePath.getObjectName(), newTableName);
    }

    public void createTable(
            ObjectPath tablePath, ResolvedCatalogTable table, boolean ignoreIfExists)
            throws PulsarAdminException, DatabaseNotExistException, TableAlreadyExistException {
        String databaseName = configuration.databaseName(tablePath.getDatabaseName());
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(catalogName, tablePath);
            }
        }

        String tableName = nameMapper.getMapping(databaseName, tablePath.getObjectName());

        // Create the related table.
        int partitions = configuration.getDefaultPartitionSize();
        admin.topics().createPartitionedTopic(tableName, partitions);

        // Upload the table schema and bypass schema check.
        SchemaInfo info = tableConverter.convert(table);
        admin.schemas().createSchema(tableName, info);
        admin.topicPolicies().setSchemaCompatibilityStrategy(tableName, ALWAYS_COMPATIBLE);
    }

    public void alterTable(
            ObjectPath tablePath, ResolvedCatalogTable table, boolean ignoreIfNotExists)
            throws PulsarAdminException, TableNotExistException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(catalogName, tablePath);
            }
        }

        String databaseName = configuration.databaseName(tablePath.getDatabaseName());
        String tableName = nameMapper.getMapping(databaseName, tablePath.getObjectName());
        SchemaInfo info = admin.schemas().getSchemaInfo(tableName);
        if (!tableConverter.isFlinkTable(info)) {
            throw new CatalogException(
                    "This table " + tablePath + " is not created by Flink, you can't modify it.");
        }

        SchemaInfo newInfo = tableConverter.convert(table);
        admin.schemas().createSchema(tableName, newInfo);
    }

    @Override
    public void close() {
        admin.close();
    }
}
