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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogOptions.NAMING_MAPPING_TOPIC;
import static org.apache.pulsar.common.naming.TopicDomain.persistent;

/**
 * This is a table name mapping for the renamed tables. The topic in Pulsar doesn't support
 * renaming. So we create an internal topic under any namespaces for recording its table name
 * mapping.
 */
class TableNameMapper {

    private final PulsarAdmin admin;

    public TableNameMapper(PulsarAdmin admin) {
        this.admin = admin;
    }

    /** Rename the given table name with a new table. */
    public void addMapping(String database, String table, String newTable)
            throws PulsarAdminException {
        String mappingTopic = internalMappingTopic(database);
        SchemaInfo info = admin.schemas().getSchemaInfo(mappingTopic);

        Map<String, String> properties = new HashMap<>(info.getProperties());
        String originalTable = properties.remove(table);
        if (originalTable != null) {
            properties.put(newTable, originalTable);
        } else {
            properties.put(newTable, table);
        }

        SchemaInfo newInfo =
                SchemaInfo.builder()
                        .name(info.getName())
                        .type(info.getType())
                        .schema(info.getSchema())
                        .properties(properties)
                        .timestamp(System.currentTimeMillis())
                        .build();
        admin.schemas().createSchema(mappingTopic, newInfo);
    }

    /** Drop the name mapping and return the original topic name. */
    public String dropMapping(String database, String table) throws PulsarAdminException {
        Map<String, String> mappingTopic = listMapping(database);
        String original = mappingTopic.remove(table);

        if (original == null) {
            original = table;
        } else {
            String topic = topicName(database, NAMING_MAPPING_TOPIC);
            SchemaInfo info = schemaInfo(mappingTopic);
            admin.schemas().createSchema(topic, info);
        }

        return topicName(database, original);
    }

    /** Get the real table name (mayn't exist) with the full path from the given database. */
    public String getMapping(String database, String table) throws PulsarAdminException {
        Map<String, String> nameMapping = listMapping(database);
        String mapping = nameMapping.getOrDefault(table, table);

        return topicName(database, mapping);
    }

    /**
     * Find all the name mapping for the given database. The key will be the table name, the value
     * will be the topic name.
     */
    public Map<String, String> listMapping(String database) throws PulsarAdminException {
        String mappingTopic = internalMappingTopic(database);
        SchemaInfo info = admin.schemas().getSchemaInfo(mappingTopic);

        return new HashMap<>(info.getProperties());
    }

    /** Create of find the internal mapping topic from the given database. */
    private String internalMappingTopic(String database) throws PulsarAdminException {
        String topic = topicName(database, NAMING_MAPPING_TOPIC);

        try {
            admin.topics().getPartitionedTopicMetadata(topic);
        } catch (NotFoundException e) {
            // Try to create the mapping tables.
            admin.topics().createNonPartitionedTopic(topic);
            // Create the initial schema info for storing the metadata.
            SchemaInfo info = schemaInfo(Collections.emptyMap());
            admin.schemas().createSchema(topic, info);
        }

        return topic;
    }

    private SchemaInfo schemaInfo(Map<String, String> mapping) {
        return SchemaInfo.builder()
                .name("FlinkTableMapping")
                .type(SchemaType.BYTES)
                .schema(new byte[0])
                .properties(mapping)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    private String topicName(String database, String table) {
        NamespaceName namespace = NamespaceName.get(database);
        TopicName topicName = TopicName.get(persistent.value(), namespace, table);

        return topicName.getPartitionedTopicName();
    }
}
