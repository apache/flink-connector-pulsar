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

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.connector.pulsar.table.testutils.PulsarTableTestUtils.collectRows;
import static org.apache.flink.connector.pulsar.table.testutils.PulsarTableTestUtils.comparedWithKeyAndOrder;
import static org.apache.flink.connector.pulsar.table.testutils.PulsarTableTestUtils.waitingExpectedResults;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT cases for the Pulsar table source and sink. It aims to verify runtime behaviour and certain
 * use cases are correct and can produce/consume the desired records as user specifies.
 */
@ExtendWith(MiniClusterExtension.class)
public class UpsertPulsarTableITCase extends PulsarTableITCase {
    private static final String USERS_TOPIC = "users";
    private static final String WORD_COUNT_TOPIC = "word_count";

    @ParameterizedTest
    @ValueSource(strings = {JSON_FORMAT})
    public void testAggregate(String format) throws Exception {
        String topic = WORD_COUNT_TOPIC + "_" + format;
        pulsar.operator().createTopic(topic, 4);
        // -------------   test   ---------------
        wordCountToUpsertPulsar(format, topic);
        wordFreqToUpsertPulsar(format, topic);
    }

    /**
     * There is a known issue when parallelism is set to 2 in the temporal join. This is related to
     * the watermark of Pulsar source. The partition number is expected to be the same as the
     * parallelism in this test.
     */
    @ParameterizedTest
    @ValueSource(strings = {JSON_FORMAT})
    public void testTemporalJoin(String format) throws Exception {
        String topic = USERS_TOPIC + "_" + format;
        pulsar.operator().createTopic(topic, 1);

        env.setParallelism(1);
        writeChangelogToUpsertPulsarWithMetadata(format, topic);
        env.setParallelism(1);
        temporalJoinUpsertPulsar(format, topic);
    }

    private void wordCountToUpsertPulsar(String format, String wordCountTable) throws Exception {
        // ------------- test data ---------------

        final List<Row> inputData =
                Arrays.stream("Good good study day day up Good boy".split(" "))
                        .map(Row::of)
                        .collect(Collectors.toList());

        // ------------- create table ---------------

        final String createSource =
                String.format(
                        "CREATE TABLE words_%s ("
                                + "  `word` STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s'"
                                + ")",
                        format, TestValuesTableFactory.registerData(inputData));
        tableEnv.executeSql(createSource);
        final String createSinkTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `word` STRING,\n"
                                + "  `count` BIGINT,\n"
                                + "  PRIMARY KEY (`word`) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-pulsar',\n"
                                + "  'topics' = '%s',\n"
                                + "  'service-url' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'value.format' = '%s'"
                                + ")",
                        wordCountTable,
                        wordCountTable,
                        pulsar.operator().serviceUrl(),
                        format,
                        format);
        tableEnv.executeSql(createSinkTable);
        String initialValues =
                "INSERT INTO "
                        + wordCountTable
                        + " "
                        + "SELECT LOWER(`word`), count(*) as `count` "
                        + "FROM words_"
                        + format
                        + " "
                        + "GROUP BY LOWER(`word`)";
        tableEnv.executeSql(initialValues).await();

        // ---------- read from the upsert sink -------------------

        final List<Row> result =
                collectRows(tableEnv.sqlQuery("SELECT * FROM " + wordCountTable), 11);

        final Map<Row, List<Row>> expected = new HashMap<>();
        expected.put(
                Row.of("good"),
                Arrays.asList(
                        changelogRow("+I", "good", 1L),
                        changelogRow("-U", "good", 1L),
                        changelogRow("+U", "good", 2L),
                        changelogRow("-U", "good", 2L),
                        changelogRow("+U", "good", 3L)));
        expected.put(Row.of("study"), Collections.singletonList(changelogRow("+I", "study", 1L)));
        expected.put(
                Row.of("day"),
                Arrays.asList(
                        changelogRow("+I", "day", 1L),
                        changelogRow("-U", "day", 1L),
                        changelogRow("+U", "day", 2L)));
        expected.put(Row.of("up"), Collections.singletonList(changelogRow("+I", "up", 1L)));
        expected.put(Row.of("boy"), Collections.singletonList(changelogRow("+I", "boy", 1L)));

        comparedWithKeyAndOrder(expected, result, new int[] {0});

        // ---------- read the raw data from pulsar -------------------
        // check we only write the upsert data into Pulsar
        String rawWordCountTable = "raw_word_count";
        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `word` STRING NOT NULL,\n"
                                + "  `count` BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'pulsar',\n"
                                + "  'topics' = '%s',\n"
                                + "  'service-url' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields' = 'word',\n"
                                + "  'value.format' = '%s'\n"
                                + ")",
                        rawWordCountTable,
                        wordCountTable,
                        pulsar.operator().serviceUrl(),
                        format,
                        format));

        final List<Row> result2 =
                collectRows(tableEnv.sqlQuery("SELECT * FROM " + rawWordCountTable), 8);
        final Map<Row, List<Row>> expected2 = new HashMap<>();
        expected2.put(
                Row.of("good"),
                Arrays.asList(Row.of("good", 1L), Row.of("good", 2L), Row.of("good", 3L)));
        expected2.put(Row.of("study"), Collections.singletonList(Row.of("study", 1L)));
        expected2.put(Row.of("day"), Arrays.asList(Row.of("day", 1L), Row.of("day", 2L)));
        expected2.put(Row.of("up"), Collections.singletonList(Row.of("up", 1L)));
        expected2.put(Row.of("boy"), Collections.singletonList(Row.of("boy", 1L)));

        comparedWithKeyAndOrder(expected2, result2, new int[] {0});
    }

    private void wordFreqToUpsertPulsar(String format, String wordCountTable) throws Exception {
        // ------------- test data ---------------

        final List<String> expectedData = Arrays.asList("+I[3, 1]", "+I[2, 1]");

        // ------------- create table ---------------

        final String createSinkTable =
                "CREATE TABLE sink_"
                        + format
                        + "(\n"
                        + "  `count` BIGINT,\n"
                        + "  `freq` BIGINT,\n"
                        + "  PRIMARY KEY (`count`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ")";
        tableEnv.executeSql(createSinkTable);
        final TableResult query =
                tableEnv.executeSql(
                        "INSERT INTO sink_"
                                + format
                                + "\n"
                                + "SELECT `count`, count(*) as `freq`\n"
                                + "FROM "
                                + wordCountTable
                                + "\n"
                                + "GROUP BY `count`\n"
                                + "having count(*) < 2");

        waitingExpectedResults("sink_" + format, expectedData, Duration.ofSeconds(20));
        query.getJobClient().get().cancel();
    }

    private void writeChangelogToUpsertPulsarWithMetadata(String format, String userTable)
            throws Exception {
        // ------------- test data ---------------

        // Prepare data for upsert pulsar
        // Keep every partition has more than 1 record and the last record in every partition has
        // larger event time
        // than left stream event time to trigger the join.
        List<Row> changelogData =
                Arrays.asList(
                        changelogRow(
                                "+U",
                                100L,
                                "Bob",
                                "Beijing",
                                LocalDateTime.parse("2020-08-15T00:00:01")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Shanghai",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        changelogRow(
                                "+U",
                                102L,
                                "Greg",
                                "Berlin",
                                LocalDateTime.parse("2020-08-15T00:00:03")),
                        changelogRow(
                                "+U",
                                103L,
                                "Richard",
                                "Berlin",
                                LocalDateTime.parse("2020-08-16T00:01:05")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Wuhan",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        changelogRow(
                                "+U",
                                104L,
                                "Tomato",
                                "Hongkong",
                                LocalDateTime.parse("2020-08-16T00:05:05")),
                        changelogRow(
                                "+U",
                                105L,
                                "Tim",
                                "Shenzhen",
                                LocalDateTime.parse("2020-08-16T00:06:00")),
                        // Keep the timestamp in the records are in the ascending order.
                        // It will keep the records in the pulsar partition are in the order.
                        // It has the same effects by adjusting the watermark strategy.
                        changelogRow(
                                "+U",
                                103L,
                                "Richard",
                                "London",
                                LocalDateTime.parse("2020-08-16T01:01:05")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Hangzhou",
                                LocalDateTime.parse("2020-08-16T01:04:05")),
                        changelogRow(
                                "+U",
                                104L,
                                "Tomato",
                                "Shenzhen",
                                LocalDateTime.parse("2020-08-16T01:05:05")),
                        changelogRow(
                                "+U",
                                105L,
                                "Tim",
                                "Hongkong",
                                LocalDateTime.parse("2020-08-16T01:06:00")));

        // ------------- create table ---------------

        final String createChangelog =
                String.format(
                        "CREATE TABLE users_changelog_%s ("
                                + "  user_id BIGINT,"
                                + "  user_name STRING,"
                                + "  region STRING,"
                                + "  modification_time TIMESTAMP(3),"
                                + "  PRIMARY KEY (user_id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s',"
                                + "  'changelog-mode' = 'UA,D',"
                                + "  'disable-lookup' = 'true'"
                                + ")",
                        format, TestValuesTableFactory.registerData(changelogData));
        tableEnv.executeSql(createChangelog);

        // we verified computed column, watermark, metadata in this case too
        final String createSinkTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `user_name` STRING,\n"
                                + "  `region` STRING,\n"
                                + "  upper_region AS UPPER(`region`),\n"
                                + "  modification_time TIMESTAMP(3) METADATA FROM 'event_time',\n"
                                + "  watermark for modification_time as modification_time,\n"
                                + "  PRIMARY KEY (`user_id`) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-pulsar',\n"
                                + "  'topics' = '%s',\n"
                                + "  'service-url' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'value.format' = '%s'"
                                + ")",
                        userTable, userTable, pulsar.operator().serviceUrl(), format, format);
        tableEnv.executeSql(createSinkTable);
        String initialValues =
                "INSERT INTO " + userTable + " " + "SELECT * " + "FROM users_changelog_" + format;
        tableEnv.executeSql(initialValues).await();

        // ---------- consume stream from sink -------------------

        final List<Row> result = collectRows(tableEnv.sqlQuery("SELECT * FROM " + userTable), 16);

        List<Row> expected =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                100L,
                                "Bob",
                                "Beijing",
                                "BEIJING",
                                LocalDateTime.parse("2020-08-15T00:00:01")),
                        changelogRow(
                                "+I",
                                101L,
                                "Alice",
                                "Shanghai",
                                "SHANGHAI",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        changelogRow(
                                "-U",
                                101L,
                                "Alice",
                                "Shanghai",
                                "SHANGHAI",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Wuhan",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        changelogRow(
                                "-U",
                                101L,
                                "Alice",
                                "Wuhan",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Hangzhou",
                                "HANGZHOU",
                                LocalDateTime.parse("2020-08-16T01:04:05")),
                        changelogRow(
                                "+I",
                                102L,
                                "Greg",
                                "Berlin",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-15T00:00:03")),
                        changelogRow(
                                "+I",
                                103L,
                                "Richard",
                                "Berlin",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-16T00:01:05")),
                        changelogRow(
                                "-U",
                                103L,
                                "Richard",
                                "Berlin",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-16T00:01:05")),
                        changelogRow(
                                "+U",
                                103L,
                                "Richard",
                                "London",
                                "LONDON",
                                LocalDateTime.parse("2020-08-16T01:01:05")),
                        changelogRow(
                                "+I",
                                104L,
                                "Tomato",
                                "Hongkong",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T00:05:05")),
                        changelogRow(
                                "-U",
                                104L,
                                "Tomato",
                                "Hongkong",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T00:05:05")),
                        changelogRow(
                                "+U",
                                104L,
                                "Tomato",
                                "Shenzhen",
                                "SHENZHEN",
                                LocalDateTime.parse("2020-08-16T01:05:05")),
                        changelogRow(
                                "+I",
                                105L,
                                "Tim",
                                "Shenzhen",
                                "SHENZHEN",
                                LocalDateTime.parse("2020-08-16T00:06")),
                        changelogRow(
                                "-U",
                                105L,
                                "Tim",
                                "Shenzhen",
                                "SHENZHEN",
                                LocalDateTime.parse("2020-08-16T00:06")),
                        changelogRow(
                                "+U",
                                105L,
                                "Tim",
                                "Hongkong",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T01:06")));
        assertThat(result).hasSameElementsAs(expected);
    }

    private void temporalJoinUpsertPulsar(String format, String userTable) throws Exception {
        // ------------- test data ---------------
        List<Row> input =
                Arrays.asList(
                        Row.of(10001L, 100L, LocalDateTime.parse("2020-08-15T00:00:02")),
                        Row.of(10002L, 101L, LocalDateTime.parse("2020-08-15T00:00:03")),
                        Row.of(10002L, 102L, LocalDateTime.parse("2020-08-15T00:00:04")),
                        Row.of(10002L, 101L, LocalDateTime.parse("2020-08-16T00:02:01")),
                        Row.of(10004L, 104L, LocalDateTime.parse("2020-08-16T00:04:00")),
                        Row.of(10003L, 101L, LocalDateTime.parse("2020-08-16T00:04:06")),
                        Row.of(10004L, 104L, LocalDateTime.parse("2020-08-16T00:05:06")));

        List<Row> expected =
                Arrays.asList(
                        Row.of(
                                10001L,
                                100L,
                                LocalDateTime.parse("2020-08-15T00:00:02"),
                                "Bob",
                                "BEIJING",
                                LocalDateTime.parse("2020-08-15T00:00:01")),
                        Row.of(
                                10002L,
                                101L,
                                LocalDateTime.parse("2020-08-15T00:00:03"),
                                "Alice",
                                "SHANGHAI",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        Row.of(
                                10002L,
                                102L,
                                LocalDateTime.parse("2020-08-15T00:00:04"),
                                "Greg",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-15T00:00:03")),
                        Row.of(
                                10002L,
                                101L,
                                LocalDateTime.parse("2020-08-16T00:02:01"),
                                "Alice",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        Row.of(
                                10004L,
                                104L,
                                LocalDateTime.parse("2020-08-16T00:04:00"),
                                null,
                                null,
                                null),
                        Row.of(
                                10003L,
                                101L,
                                LocalDateTime.parse("2020-08-16T00:04:06"),
                                "Alice",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        Row.of(
                                10004L,
                                104L,
                                LocalDateTime.parse("2020-08-16T00:05:06"),
                                "Tomato",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T00:05:05")));

        // ------------- create table ---------------

        tableEnv.executeSql(
                String.format(
                        "CREATE TABLE pageviews_%s(\n"
                                + "  `page_id` BIGINT,\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `viewtime` TIMESTAMP(3),\n"
                                + "  `proctime` as proctime(),\n"
                                + "   watermark for `viewtime` as `viewtime`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s'\n"
                                + ")",
                        format, TestValuesTableFactory.registerData(input)));
        final List<Row> result =
                collectRows(
                        tableEnv.sqlQuery(
                                String.format(
                                        "SELECT p.page_id, p.user_id, p.viewtime, u.user_name, u.upper_region, u.modification_time\n"
                                                + "FROM pageviews_%s AS p\n"
                                                + "LEFT JOIN %s FOR SYSTEM_TIME AS OF p.viewtime AS u\n"
                                                + "ON p.user_id = u.user_id",
                                        format, userTable)),
                        7);
        assertThat(result).hasSameElementsAs(expected);
    }
}
