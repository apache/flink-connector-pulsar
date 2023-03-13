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
import org.apache.flink.connector.pulsar.common.config.PulsarConfigBuilder;
import org.apache.flink.connector.pulsar.table.catalog.client.PulsarCatalogClient;
import org.apache.flink.connector.pulsar.table.catalog.config.CatalogConfiguration;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.LOCAL_PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.LOCAL_PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.catalog.config.PulsarCatalogConfigUtils.CATALOG_VALIDATOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * Catalog implementation which uses Pulsar to store Flink created databases/tables, exposes the
 * Pulsar's namespace as the Flink databases and exposes the Pulsar's topics as the Flink tables.
 *
 * <h2>Database Mapping</h2>
 *
 * <p>{@link PulsarCatalog} offers two kinds of databases.
 *
 * <ul>
 *   <li><strong>Managed Databases</strong><br>
 *       A managed database refers to a database created by using Flink but the its name doesn't
 *       contain tenant information.<br>
 *       We will created the corresponding namespace under the tenant configured by {@link
 *       PulsarCatalogOptions#PULSAR_CATALOG_MANAGED_TENANT}.
 *   <li><strong>Pulsar Databases</strong><br>
 *       A Pulsar databases refers to an existing namespace that wasn't a system namespace nor under
 *       the Flink managed tenant in Pulsar. Each namespace will be mapped to a database using the
 *       tenant and namespace name like {@code tenant/namespace}.
 * </ul>
 *
 * <h2>Table Mapping</h2>
 *
 * <p>A table refers to a Pulsar topic, using a 1-to-1 mapping from the Pulsar's {@link
 * TopicDomain#persistent} topic to the Flink table. We don't support {@link
 * TopicDomain#non_persistent} topics here.
 *
 * <p>Each topic will be mapped to a table under a database using the topic's tenant and namespace
 * named like {@code tenant/namespace}. The mapped table has the same name as the local name of the
 * original topic. For example, the topic {@code persistent://public/default/some} will be mapped to
 * {@code some} table under the {@code public/default} database. This allows users to easily query
 * from existing Pulsar topics without explicitly creating the table. It automatically determines
 * the Flink format to use based on the stored Pulsar schema in the Pulsar topic.
 *
 * <p>This mapping has some limitations, such as users can't designate the watermark and thus can't
 * use window aggregate functions for the topics that aren't created by catalog.
 */
@PublicEvolving
@SuppressWarnings("java:S1192")
public class PulsarCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCatalog.class);

    private final CatalogConfiguration configuration;

    private PulsarCatalogClient client;

    public PulsarCatalog(String catalogName, CatalogConfiguration configuration) {
        super(catalogName, configuration.getDefaultDatabase());

        PulsarConfigBuilder builder = new PulsarConfigBuilder(configuration);

        // Set the required options for supporting the local catalog.
        builder.setIfMissing(PULSAR_SERVICE_URL, LOCAL_PULSAR_SERVICE_URL);
        builder.setIfMissing(PULSAR_ADMIN_URL, LOCAL_PULSAR_ADMIN_URL);

        // We may create the CatalogConfiguration twice when using the PulsarCatalogFactory.
        // But we truly add the config validation when you want to manually create the Pulsar
        // catalog.
        this.configuration = builder.build(CATALOG_VALIDATOR, CatalogConfiguration::new);
    }

    @Override
    public Optional<Factory> getFactory() {
        // We will add PulsarDynamicTableFactory support here in the upcoming PRs.
        return Optional.empty();
    }

    @Override
    public void open() throws CatalogException {
        // Create the catalog client.
        if (client == null) {
            try {
                this.client = new PulsarCatalogClient(getName(), configuration);
            } catch (PulsarClientException e) {
                String message =
                        "Failed to create the client in catalog "
                                + getName()
                                + ", config is: "
                                + configuration;
                throw new CatalogException(message, e);
            }
        }

        // Create the flink managed tenant.
        try {
            client.initializeManagedTenant();
        } catch (PulsarAdminException e) {
            String managedTenant = configuration.getManagedTenant();
            String message =
                    "Failed to initialize the Flink managed tenant: "
                            + managedTenant
                            + " in catalog: "
                            + getName()
                            + " with config: "
                            + configuration;
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (client != null) {
            client.close();
            LOG.debug("Successfully close the catalog client.");
        }
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return client.listDatabases();
        } catch (PulsarAdminException e) {
            String message = "Failed to list the databases in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return client.getDatabase(databaseName);
        } catch (PulsarAdminException e) {
            String message =
                    "Failed to query the given database: "
                            + databaseName
                            + " in catalog: "
                            + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return client.databaseExists(databaseName);
        } catch (PulsarAdminException e) {
            String message =
                    "Failed to check if database: "
                            + databaseName
                            + " exists in catalog: "
                            + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            client.createDatabase(name, database, ignoreIfExists);
        } catch (PulsarAdminException e) {
            String message = "Failed to create database: " + name + " in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            client.dropDatabase(name, ignoreIfNotExists, cascade);
        } catch (PulsarAdminException e) {
            String message = "Failed to drop database: " + name + " in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try {
            client.alterDatabase(name, newDatabase, ignoreIfNotExists);
        } catch (PulsarAdminException e) {
            String message = "Failed to alter database: " + name + " in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    // ------ tables and views ------

    @Override
    public List<String> listTables(String name) throws DatabaseNotExistException, CatalogException {
        try {
            return client.listTables(name);
        } catch (PulsarAdminException e) {
            String message =
                    "Failed to list tables of database: " + name + " in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public List<String> listViews(String name) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support view");
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            return client.tableExists(tablePath);
        } catch (PulsarAdminException e) {
            String message =
                    "Failed to check if table: " + tablePath + " exists in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");

        try {
            client.dropTable(tablePath, ignoreIfNotExists);
        } catch (PulsarAdminException e) {
            String message = "Failed to delete table: " + tablePath + " in catalog: " + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkArgument(
                !isNullOrWhitespaceOnly(newTableName), "newTableName cannot be null or empty");

        try {
            client.renameTable(tablePath, newTableName, ignoreIfNotExists);
        } catch (PulsarAdminException e) {
            String message =
                    "Failed to rename table: "
                            + tablePath
                            + " to new name: "
                            + newTableName
                            + " in catalog: "
                            + getName();
            throw new CatalogException(message, e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(table, "table cannot be null");

        if (table instanceof ResolvedCatalogTable) {
            try {
                client.createTable(tablePath, (ResolvedCatalogTable) table, ignoreIfExists);
            } catch (PulsarAdminException e) {
                String message =
                        "Failed to create table: " + tablePath + " in catalog: " + getName();
                throw new CatalogException(message, e);
            }
        } else if (table instanceof ResolvedCatalogView) {
            throw new UnsupportedOperationException("Pulsar catalog don't support view");
        } else {
            throw new UnsupportedOperationException(
                    "We don't support such kind of table: " + table.getClass());
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(newTable, "newCatalogTable cannot be null");

        if (newTable instanceof ResolvedCatalogTable) {
            try {
                client.alterTable(tablePath, (ResolvedCatalogTable) newTable, ignoreIfNotExists);
            } catch (PulsarAdminException e) {
                String message =
                        "Failed to alter table: " + tablePath + " in catalog: " + getName();
                throw new CatalogException(message, e);
            }
        } else if (newTable instanceof ResolvedCatalogView) {
            throw new UnsupportedOperationException("Pulsar catalog don't support view");
        } else {
            throw new UnsupportedOperationException(
                    "We don't support such kind of table: " + newTable.getClass());
        }
    }

    @Override
    public boolean supportsManagedTable() {
        return true;
    }

    // ------------------------------------------------------------------------
    // Unsupported catalog operations for Pulsar
    // ------------------------------------------------------------------------

    // ------ partitions ------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support partition for now");
    }

    // ------ functions ------

    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support function for now");
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support function for now");
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support function for now");
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support function for now");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support function for now");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support function for now");
    }

    // ------ statistics ------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException("Pulsar catalog don't support statistic for now");
    }
}
