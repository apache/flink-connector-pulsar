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

package org.apache.flink.connector.pulsar.table;

import static org.apache.flink.connector.pulsar.table.testutils.PulsarTableTestUtils.readLines;
import static org.apache.flink.connector.pulsar.table.testutils.PulsarTableTestUtils.waitingExpectedResults;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.formats.json.canal.CanalJsonFormatFactory;
import org.apache.flink.formats.json.debezium.DebeziumJsonFormatFactory;
import org.apache.flink.formats.json.maxwell.MaxwellJsonFormatFactory;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** IT cases for Pulsar with changelog format for Table API & SQL. */
@ExtendWith(MiniClusterExtension.class)
public class PulsarChangelogTableITCase extends PulsarTableTestBase {
    @Test
    void testPulsarDebeziumChangelogSource() throws Exception {
        final String topic = "changelog_topic";
        pulsar.operator().createTopic(topic, 1);

        // enables MiniBatch processing to verify MiniBatch + FLIP-95, see FLINK-18769
        TableConfig tableConf = tableEnv.getConfig();
        tableConf.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tableConf.set(
                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1));
        tableConf.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5000L);
        tableConf.set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE");

        // ---------- Write the Debezium json into Pulsar -------------------
        List<String> lines = readLines("debezium-data-schema-exclude.txt");
        try {
            writeRecordsToPulsar(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Pulsar.", e);
        }

        // ---------- Produce an event time stream into Pulsar -------------------
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                // test format metadata
                                //                                + " origin_ts TIMESTAMP(3)
                                // METADATA FROM 'value.ingestion-timestamp' VIRTUAL," // unused
                                + " origin_table STRING METADATA FROM 'value.source.table' VIRTUAL,"
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " origin_topic STRING METADATA FROM 'topic' VIRTUAL"
                                + ") WITH ("
                                + " 'connector' = '%s',"
                                + " 'topics' = '%s',"
                                + " 'service-url' = '%s',\n"
                                + " 'admin-url' = '%s',\n"
                                + " 'value.format' = '%s',"
                                + " 'pulsar.source.fetchOneMessageTime' = '100'"
                                + ")",
                        PulsarTableFactory.IDENTIFIER,
                        topic,
                        pulsar.operator().serviceUrl(),
                        pulsar.operator().adminUrl(),
                        DebeziumJsonFormatFactory.IDENTIFIER);
        String sinkDDL =
                "CREATE TABLE debezium_sink ("
                        + " origin_topic STRING,"
                        + " origin_table STRING,"
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        TableResult tableResult =
                tableEnv.executeSql(
                        "INSERT INTO debezium_sink "
                                + "SELECT FIRST_VALUE(origin_topic), FIRST_VALUE(origin_table), name, SUM(weight) "
                                + "FROM debezium_source GROUP BY name");
        /*
         * Debezium captures change data on the `products` table:
         *
         * <pre>
         * CREATE TABLE products (
         *  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
         *  name VARCHAR(255),
         *  description VARCHAR(512),
         *  weight FLOAT
         * );
         * ALTER TABLE products AUTO_INCREMENT = 101;
         *
         * INSERT INTO products
         * VALUES (default,"scooter","Small 2-wheel scooter",3.14),
         *        (default,"car battery","12V car battery",8.1),
         *        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
         *        (default,"hammer","12oz carpenter's hammer",0.75),
         *        (default,"hammer","14oz carpenter's hammer",0.875),
         *        (default,"hammer","16oz carpenter's hammer",1.0),
         *        (default,"rocks","box of assorted rocks",5.3),
         *        (default,"jacket","water resistent black wind breaker",0.1),
         *        (default,"spare tire","24 inch spare tire",22.2);
         * UPDATE products SET description='18oz carpenter hammer' WHERE id=106;
         * UPDATE products SET weight='5.1' WHERE id=107;
         * INSERT INTO products VALUES (default,"jacket","water resistent white wind breaker",0.2);
         * INSERT INTO products VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
         * UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
         * UPDATE products SET weight='5.17' WHERE id=111;
         * DELETE FROM products WHERE id=111;
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | id  | name               | description                                             | weight |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |
         * | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
         * | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */
        List<String> expected =
                Arrays.asList(
                        "+I[persistent://public/default/changelog_topic-partition-0, products, scooter, 3.140]",
                        "+I[persistent://public/default/changelog_topic-partition-0, products, car battery, 8.100]",
                        "+I[persistent://public/default/changelog_topic-partition-0, products, 12-pack drill bits, 0.800]",
                        "+I[persistent://public/default/changelog_topic-partition-0, products, hammer, 2.625]",
                        "+I[persistent://public/default/changelog_topic-partition-0, products, rocks, 5.100]",
                        "+I[persistent://public/default/changelog_topic-partition-0, products, jacket, 0.600]",
                        "+I[persistent://public/default/changelog_topic-partition-0, products, spare tire, 22.200]");

        waitingExpectedResults("debezium_sink", expected, Duration.ofSeconds(10));

        // ------------- cleanup -------------------

        tableResult.getJobClient().get().cancel().get(); // stop the job
    }

    @Test
    public void testPulsarCanalChangelogSource() throws Exception {
        final String topic = "changelog_canal";
        pulsar.operator().createTopic(topic, 1);

        // configure time zone of  the Canal Json metadata "ingestion-timestamp"
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        // enables MiniBatch processing to verify MiniBatch + FLIP-95, see FLINK-18769
        TableConfig tableConf = tableEnv.getConfig();
        tableConf.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tableConf.set(
                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1));
        tableConf.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5000L);
        tableConf.set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE");

        // ---------- Write the Canal json into Pulsar -------------------
        List<String> lines = readLines("canal-data.txt");
        try {
            writeRecordsToPulsar(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Pulsar.", e);
        }

        // ---------- Produce an event time stream into Pulsar -------------------
        String sourceDDL =
                String.format(
                        "CREATE TABLE canal_source ("
                                // test format metadata
                                + " origin_database STRING METADATA FROM 'value.database' VIRTUAL,"
                                + " origin_table STRING METADATA FROM 'value.table' VIRTUAL,"
                                + " origin_sql_type MAP<STRING, INT> METADATA FROM 'value.sql-type' VIRTUAL,"
                                + " origin_pk_names ARRAY<STRING> METADATA FROM 'value.pk-names' VIRTUAL,"
                                + " origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,"
                                + " origin_es TIMESTAMP(3) METADATA FROM 'value.event-timestamp' VIRTUAL,"
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                // test connector metadata
                                + " origin_topic STRING METADATA FROM 'topic' VIRTUAL,"
                                + " WATERMARK FOR origin_es AS origin_es - INTERVAL '5' SECOND"
                                + ") WITH ("
                                + " 'connector' = '%s',"
                                + " 'topics' = '%s',"
                                + " 'service-url' = '%s',\n"
                                + " 'admin-url' = '%s',\n"
                                + " 'value.format' = 'canal-json',"
                                + " 'pulsar.source.fetchOneMessageTime' = '100'"
                                + ")",
                        PulsarTableFactory.IDENTIFIER,
                        topic,
                        pulsar.operator().serviceUrl(),
                        pulsar.operator().adminUrl(),
                        CanalJsonFormatFactory.IDENTIFIER);
        String sinkDDL =
                "CREATE TABLE canal_sink ("
                        + " origin_topic STRING,"
                        + " origin_database STRING,"
                        + " origin_table STRING,"
                        + " origin_sql_type MAP<STRING, INT>,"
                        + " origin_pk_names ARRAY<STRING>,"
                        + " origin_ts TIMESTAMP(3),"
                        + " origin_es TIMESTAMP(3),"
                        + " name STRING,"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        TableResult tableResult =
                tableEnv.executeSql(
                        "INSERT INTO canal_sink "
                                + "SELECT origin_topic, origin_database, origin_table, origin_sql_type, "
                                + "origin_pk_names, origin_ts, origin_es, name "
                                + "FROM canal_source");

        /*
         * Canal captures change data on the `products` table:
         *
         * <pre>
         * CREATE TABLE products (
         *  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
         *  name VARCHAR(255),
         *  description VARCHAR(512),
         *  weight FLOAT
         * );
         * ALTER TABLE products AUTO_INCREMENT = 101;
         *
         * INSERT INTO products
         * VALUES (default,"scooter","Small 2-wheel scooter",3.14),
         *        (default,"car battery","12V car battery",8.1),
         *        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
         *        (default,"hammer","12oz carpenter's hammer",0.75),
         *        (default,"hammer","14oz carpenter's hammer",0.875),
         *        (default,"hammer","16oz carpenter's hammer",1.0),
         *        (default,"rocks","box of assorted rocks",5.3),
         *        (default,"jacket","water resistent black wind breaker",0.1),
         *        (default,"spare tire","24 inch spare tire",22.2);
         * UPDATE products SET description='18oz carpenter hammer' WHERE id=106;
         * UPDATE products SET weight='5.1' WHERE id=107;
         * INSERT INTO products VALUES (default,"jacket","water resistent white wind breaker",0.2);
         * INSERT INTO products VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
         * UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
         * UPDATE products SET weight='5.17' WHERE id=111;
         * DELETE FROM products WHERE id=111;
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | id  | name               | description                                             | weight |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |
         * | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
         * | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */

        List<String> expected =
                Arrays.asList(
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:38:35.477, 2020-05-13T12:38:35, 12-pack drill bits]",
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:38:35.477, 2020-05-13T12:38:35, spare tire]",
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:39:06.301, 2020-05-13T12:39:06, hammer]",
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:39:09.489, 2020-05-13T12:39:09, rocks]",
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:39:18.230, 2020-05-13T12:39:18, jacket]",
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:42:33.939, 2020-05-13T12:42:33, car battery]",
                        "+I[persistent://public/default/changelog_canal-partition-0, inventory, products2, {name=12, weight=7, description=12, id=4}, [id], 2020-05-13T12:42:33.939, 2020-05-13T12:42:33, scooter]");

        waitingExpectedResults("canal_sink", expected, Duration.ofSeconds(10));

        // ------------- cleanup -------------------

        tableResult.getJobClient().get().cancel().get(); // stop the job
    }

    @Test
    public void testPulsarMaxwellChangelogSource() throws Exception {
        final String topic = "changelog_maxwell";
        pulsar.operator().createTopic(topic, 1);

        // configure time zone of  the Maxwell Json metadata "ingestion-timestamp"
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        // enables MiniBatch processing to verify MiniBatch + FLIP-95, see FLINK-18769
        TableConfig tableConf = tableEnv.getConfig();
        tableConf.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tableConf.set(
                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1));
        tableConf.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5000L);
        tableConf.set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE");

        // ---------- Write the Maxwell json into Pulsar -------------------
        List<String> lines = readLines("maxwell-data.txt");
        try {
            writeRecordsToPulsar(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write maxwell data to Pulsar.", e);
        }

        // ---------- Produce an event time stream into Pulsar -------------------
        String sourceDDL =
                String.format(
                        "CREATE TABLE maxwell_source ("
                                // test format metadata
                                + " origin_database STRING METADATA FROM 'value.database' VIRTUAL,"
                                + " origin_table STRING METADATA FROM 'value.table' VIRTUAL,"
                                + " origin_primary_key_columns ARRAY<STRING> METADATA FROM 'value.primary-key-columns' VIRTUAL,"
                                + " origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,"
                                + " id INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                // test connector metadata
                                + " origin_topic STRING METADATA FROM 'topic' VIRTUAL"
                                + ") WITH ("
                                + " 'connector' = '%s',"
                                + " 'topics' = '%s',"
                                + " 'service-url' = '%s',\n"
                                + " 'admin-url' = '%s',\n"
                                + " 'value.format' = '%s',"
                                + " 'pulsar.source.fetchOneMessageTime' = '100'"
                                + ")",
                        PulsarTableFactory.IDENTIFIER,
                        topic,
                        pulsar.operator().serviceUrl(),
                        pulsar.operator().adminUrl(),
                        MaxwellJsonFormatFactory.IDENTIFIER);
        String sinkDDL =
                "CREATE TABLE maxwell_sink ("
                        + " origin_topic STRING,"
                        + " origin_database STRING,"
                        + " origin_table STRING,"
                        + " origin_primary_key_columns ARRAY<STRING>,"
                        + " origin_ts TIMESTAMP(3),"
                        + " name STRING,"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        TableResult tableResult =
                tableEnv.executeSql(
                        "INSERT INTO maxwell_sink "
                                + "SELECT origin_topic, origin_database, origin_table, origin_primary_key_columns, "
                                + "origin_ts, name "
                                + "FROM maxwell_source");

        /*
         * Maxwell captures change data on the `products` table:
         *
         * <pre>
         * CREATE TABLE products (
         *  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
         *  name VARCHAR(255),
         *  description VARCHAR(512),
         *  weight FLOAT
         * );
         * ALTER TABLE products AUTO_INCREMENT = 101;
         *
         * INSERT INTO products
         * VALUES (default,"scooter","Small 2-wheel scooter",3.14),
         *        (default,"car battery","12V car battery",8.1),
         *        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
         *        (default,"hammer","12oz carpenter's hammer",0.75),
         *        (default,"hammer","14oz carpenter's hammer",0.875),
         *        (default,"hammer","16oz carpenter's hammer",1.0),
         *        (default,"rocks","box of assorted rocks",5.3),
         *        (default,"jacket","water resistent black wind breaker",0.1),
         *        (default,"spare tire","24 inch spare tire",22.2);
         * UPDATE products SET description='18oz carpenter hammer' WHERE id=106;
         * UPDATE products SET weight='5.1' WHERE id=107;
         * INSERT INTO products VALUES (default,"jacket","water resistent white wind breaker",0.2);
         * INSERT INTO products VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
         * UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
         * UPDATE products SET weight='5.17' WHERE id=111;
         * DELETE FROM products WHERE id=111;
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | id  | name               | description                                             | weight |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |
         * | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
         * | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */

        List<String> expected =
                Arrays.asList(
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:34:43, 12-pack drill bits]",
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:34:43, spare tire]",
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:34:53, hammer]",
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:34:57, rocks]",
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:35:06, jacket]",
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:35:28, car battery]",
                        "+I[persistent://public/default/changelog_maxwell-partition-0, test, product, null, 2020-08-06T03:35:28, scooter]");

        waitingExpectedResults("maxwell_sink", expected, Duration.ofSeconds(10));

        // ------------- cleanup -------------------

        tableResult.getJobClient().get().cancel().get(); // stop the job
    }

    private void writeRecordsToPulsar(String topic, List<String> lines) throws Exception {
        pulsar.operator().sendMessages(topic, Schema.STRING, lines);
    }
}
