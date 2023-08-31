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

package org.apache.flink.tests.util.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.connector.pulsar.table.PulsarTableFactory;
import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.pulsar.common.PulsarContainerTestEnvironment;
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end test for the pulsar SQL connectors. */
@Category(value = {FailsOnJava11.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestLoggerExtension.class)
public class SQLClientPulsarE2ECase {

    private static final Logger LOG = LoggerFactory.getLogger(SQLClientPulsarE2ECase.class);

    private static final String PULSAR_E2E_SQL = "pulsar_e2e.sql";

    private static final FlinkContainerTestEnvironment flink =
            new FlinkContainerTestEnvironment(1, 6);

    private static final PulsarContainerTestEnvironment pulsar =
            new PulsarContainerTestEnvironment(flink);

    private static final Path sqlAvroJar = ResourceTestUtils.getResource(".*avro.jar");
    private static final Path sqlToolBoxJar = ResourceTestUtils.getResource(".*SqlToolbox.jar");

    private final List<Path> apacheAvroJars = new ArrayList<>();

    @BeforeAll
    public void beforeAll() throws Exception {
        flink.startUp();
        pulsar.startUp();
    }

    @AfterAll
    public void afterAll() throws Exception {
        pulsar.tearDown();
        flink.tearDown();
    }

    @ParameterizedTest
    @MethodSource("providePulsarParams")
    public void testPulsar(String pulsarSQLVersion, String factoryIdentifier, String sqlJarPath)
            throws Exception {
        String testJsonTopic = "test-json-" + pulsarSQLVersion + "-" + UUID.randomUUID();
        String testAvroTopic = "test-avro-" + pulsarSQLVersion + "-" + UUID.randomUUID();
        String testResultTopic = "test-result-" + pulsarSQLVersion + "-" + UUID.randomUUID();

        pulsar.operator().createTopic(testJsonTopic, 1);
        pulsar.operator().createTopic(testResultTopic, 1);
        String[] messages =
                new String[] {
                    "{\"rowtime\": \"2018-03-12 08:00:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 08:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:00:00\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:20:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
                    "{\"rowtime\": \"2018-03-12 10:40:00\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
                };
        pulsar.operator().sendMessages(testJsonTopic, Schema.STRING, Arrays.asList(messages));

        // Create topic test-avro
        pulsar.operator().createTopic(testAvroTopic, 1);

        // Initialize the SQL statements from "pulsar_e2e.sql" file
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("$PULSAR_IDENTIFIER", factoryIdentifier);
        varsMap.put("$TOPIC_JSON_NAME", testJsonTopic);
        varsMap.put("$TOPIC_AVRO_NAME", testAvroTopic);
        varsMap.put("$TOPIC_RESULT_NAME", testResultTopic);
        varsMap.put("$PULSAR_SERVICE_URL", pulsar.operator().serviceUrl());
        varsMap.put("$PULSAR_ADMIN_URL", pulsar.operator().adminUrl());
        List<String> sqlLines = initializeSqlLines(varsMap);

        // Execute SQL statements in "pulsar_e2e.sql" file
        executeSqlStatements(sqlLines, sqlJarPath);

        // Wait until all the results flushed to the CSV file.
        LOG.info("Verify the CSV result.");
        checkResultTopic(testResultTopic);
    }

    private void executeSqlStatements(List<String> sqlLines, String sqlJarPath)
            throws IOException, InterruptedException {
        LOG.info("Executing Pulsar end-to-end SQL statements.");
        flink.getFlinkContainers()
                .submitSQLJob(
                        new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                                .addJar(sqlAvroJar)
                                .addJars(apacheAvroJars)
                                .addJar(ResourceTestUtils.getResource(sqlJarPath))
                                .addJar(sqlToolBoxJar)
                                .build());
    }

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = SQLClientPulsarE2ECase.class.getClassLoader().getResource(PULSAR_E2E_SQL);
        if (url == null) {
            throw new FileNotFoundException(PULSAR_E2E_SQL);
        }

        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }

        return result;
    }

    private void checkResultTopic(String resultTopic) throws Exception {
        List<Message<byte[]>> result =
                pulsar.operator().receiveMessages(resultTopic, Schema.BYTES, 4);
        List<String> results =
                result.stream()
                        .map(Message::getValue)
                        .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                        .collect(Collectors.toList());
        assertThat(results)
                .containsExactlyInAnyOrder(
                        "\"2018-03-12 08:00:00.000\",Alice,\"This was a warning.\",2,\"Success constant folding.\"",
                        "\"2018-03-12 09:00:00.000\",Bob,\"This was another warning.\",1,\"Success constant folding.\"",
                        "\"2018-03-12 09:00:00.000\",Steve,\"This was another info.\",2,\"Success constant folding.\"",
                        "\"2018-03-12 09:00:00.000\",Alice,\"This was a info.\",1,\"Success constant folding.\"");
    }

    static Stream<Arguments> providePulsarParams() {
        return Stream.of(Arguments.of("3.0.1", PulsarTableFactory.IDENTIFIER, ".*sql-pulsar.jar"));
    }
}
