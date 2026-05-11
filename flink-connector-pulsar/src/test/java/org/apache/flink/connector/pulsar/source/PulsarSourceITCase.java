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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.connector.pulsar.common.MiniClusterTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContextFactory;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.source.cases.EncryptedMessagesConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.MultipleTopicsConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.PartialKeysConsumingContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.SingleTopicConsumingContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.CloseableIterator;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

/**
 * Unit test class for {@link PulsarSource}. Used for {@link SubscriptionType#Exclusive}
 * subscription.
 */
class PulsarSourceITCase extends SourceTestSuiteBase<String> {

    private static final long RESULT_ASSERTION_TIMEOUT_SECONDS = 120;

    // Defines test environment on Flink MiniCluster
    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines pulsar running environment
    @TestExternalSystem
    PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.container());

    // This field is preserved, we don't support the semantics in source currently.
    @TestSemantics CheckpointingMode[] semantics = new CheckpointingMode[] {EXACTLY_ONCE};

    // Defines an external context Factories,
    // so test cases will be invoked using these external contexts.
    @TestContext
    PulsarTestContextFactory<String, SingleTopicConsumingContext> singleTopic =
            new PulsarTestContextFactory<>(pulsar, SingleTopicConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, MultipleTopicsConsumingContext> multipleTopic =
            new PulsarTestContextFactory<>(pulsar, MultipleTopicsConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, PartialKeysConsumingContext> partialKeys =
            new PulsarTestContextFactory<>(pulsar, PartialKeysConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, EncryptedMessagesConsumingContext> encryptMessages =
            new PulsarTestContextFactory<>(pulsar, EncryptedMessagesConsumingContext::new);

    @Override
    protected void checkResultWithSemantic(
            CloseableIterator<String> resultIterator,
            List<List<String>> testData,
            org.apache.flink.core.execution.CheckpointingMode semantic,
            Integer limit) {
        if (limit == null) {
            super.checkResultWithSemantic(resultIterator, testData, semantic, null);
            return;
        }

        AtomicInteger receivedRecords = new AtomicInteger();
        AtomicReference<String> progress = new AtomicReference<>("no records received yet");
        CompletableFuture<Void> assertion =
                CompletableFuture.runAsync(
                        () ->
                                assertLimitedSourceResult(
                                        resultIterator,
                                        testData,
                                        semantic,
                                        limit,
                                        receivedRecords,
                                        progress));

        try {
            assertion.get(RESULT_ASSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            try {
                resultIterator.close();
            } catch (Exception closeException) {
                e.addSuppressed(closeException);
            }
            assertion.cancel(true);
            throw new AssertionError(
                    String.format(
                            "Timed out after %d seconds while waiting for %d source records; "
                                    + "received %d records, progress: %s",
                            RESULT_ASSERTION_TIMEOUT_SECONDS,
                            limit,
                            receivedRecords.get(),
                            progress.get()),
                    e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof AssertionError) {
                throw (AssertionError) cause;
            }
            throw new AssertionError("Failed to assert source records", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while asserting source records", e);
        } catch (Exception e) {
            throw new AssertionError("Failed to assert source records", e);
        }
    }

    private static void assertLimitedSourceResult(
            CloseableIterator<String> resultIterator,
            List<List<String>> testData,
            org.apache.flink.core.execution.CheckpointingMode semantic,
            int limit,
            AtomicInteger receivedRecords,
            AtomicReference<String> progress) {
        List<SplitProgress> splits = toSplitProgress(testData);
        int totalRecords = testData.stream().mapToInt(List::size).sum();
        if (limit > totalRecords) {
            throw new IllegalArgumentException(
                    "Limit validation size should be less than total number of records "
                            + "from source");
        }

        int matchedRecords = 0;

        while (matchedRecords < limit) {
            if (!resultIterator.hasNext()) {
                throw new AssertionError(
                        String.format(
                                "Expected %d source records, but iterator ended after %d "
                                        + "records.%n%s",
                                limit, receivedRecords.get(), describeProgress(splits)));
            }

            String record = resultIterator.next();
            receivedRecords.incrementAndGet();
            int matchedSplit = matchThenForward(splits, record);
            if (matchedSplit >= 0) {
                matchedRecords++;
                progress.set(
                        String.format(
                                "matched record %d/%d from split %d: %s",
                                matchedRecords, limit, matchedSplit, record));
            } else if (semantic == org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE
                    && containsRecord(testData, record)) {
                progress.set(
                        String.format(
                                "accepted duplicate record after %d matched records: %s",
                                matchedRecords, record));
            } else {
                throw new AssertionError(
                        String.format(
                                "Unexpected record '%s' at received position %d.%n%s",
                                record, receivedRecords.get() - 1, describeProgress(splits)));
            }
        }
    }

    private static List<SplitProgress> toSplitProgress(List<List<String>> testData) {
        List<SplitProgress> splits = new ArrayList<>();
        for (List<String> split : testData) {
            splits.add(new SplitProgress(split));
        }
        return splits;
    }

    private static int matchThenForward(List<SplitProgress> splits, String record) {
        for (int i = 0; i < splits.size(); i++) {
            SplitProgress split = splits.get(i);
            if (split.hasNext() && record.equals(split.current())) {
                split.forward();
                return i;
            }
        }
        return -1;
    }

    private static boolean containsRecord(List<List<String>> testData, String record) {
        for (List<String> split : testData) {
            if (split.contains(record)) {
                return true;
            }
        }
        return false;
    }

    private static String describeProgress(List<SplitProgress> splits) {
        StringBuilder builder = new StringBuilder("Current split validation progress:");
        for (int i = 0; i < splits.size(); i++) {
            SplitProgress split = splits.get(i);
            builder.append(System.lineSeparator())
                    .append("Split ")
                    .append(i)
                    .append(" (")
                    .append(split.offset)
                    .append("/")
                    .append(split.records.size())
                    .append(")");
            if (split.hasNext()) {
                builder.append(", next expected: ").append(split.records());
            }
        }
        return builder.toString();
    }

    private static class SplitProgress {
        private final List<String> records;
        private int offset;

        private SplitProgress(List<String> records) {
            this.records = records;
        }

        private boolean hasNext() {
            return offset < records.size();
        }

        private String current() {
            return records.get(offset);
        }

        private String records() {
            return records.toString();
        }

        private void forward() {
            offset++;
        }
    }
}
