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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.core.execution.CheckpointingMode;

import org.assertj.core.api.AbstractAssert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.streaming.api.CheckpointingMode.convertToCheckpointingMode;

/**
 * Difference with {@link org.apache.flink.connector.testframe.utils.CollectIteratorAssertions}
 * CollectIteratorAssertions will generate a mismatch description if the result does not match the
 * expected value. It attempts to capture all following messages, even though already failed, which
 * helps engineers for troubleshooting, but draining the following messages may lead the test to get
 * stuck.
 *
 * <p>The current implementation will immediately fail once a message does not match, skips to drain
 * the following messages to build the information that describes failure.
 */
public class SimpleCollectIteratorAssert<T>
        extends AbstractAssert<SimpleCollectIteratorAssert<T>, Iterator<T>> {

    private final Iterator<T> collectorIterator;
    private final List<RecordsFromSplit<T>> expectedRecordsFromSplits = new ArrayList<>();
    private int totalNumRecords;
    private Integer limit = null;

    public SimpleCollectIteratorAssert(Iterator<T> collectorIterator) {
        super(collectorIterator, SimpleCollectIteratorAssert.class);
        this.collectorIterator = collectorIterator;
    }

    public SimpleCollectIteratorAssert<T> withNumRecordsLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * This method is required for downstream projects e.g. Flink connectors extending this test for
     * the case when there should be supported Flink versions below 1.20. Could be removed together
     * with dropping support for Flink 1.19.
     */
    @Deprecated
    public void matchesRecordsFromSource(
            List<List<T>> recordsBySplitsFromSource,
            org.apache.flink.streaming.api.CheckpointingMode semantic) {
        matchesRecordsFromSource(recordsBySplitsFromSource, convertToCheckpointingMode(semantic));
    }

    public void matchesRecordsFromSource(List<List<T>> expectedData, CheckpointingMode semantic) {
        for (List<T> recordsFromSplit : expectedData) {
            expectedRecordsFromSplits.add(new RecordsFromSplit<>(recordsFromSplit));
            totalNumRecords += recordsFromSplit.size();
        }

        if (limit != null && limit > totalNumRecords) {
            throw new IllegalArgumentException(
                    "Limit validation size should be less than total number of records from source");
        }

        switch (semantic) {
            case AT_LEAST_ONCE:
                compareWithAtLeastOnceSemantic(collectorIterator, expectedRecordsFromSplits);
                break;
            case EXACTLY_ONCE:
                compareWithExactlyOnceSemantic(collectorIterator, expectedRecordsFromSplits);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unrecognized semantic \"%s\"", semantic));
        }
    }

    private void compareWithAtLeastOnceSemantic(
            Iterator<T> resultIterator, List<RecordsFromSplit<T>> recordsFromSplits) {
        List<T> duplicateRead = new LinkedList<>();

        int recordCounter = 0;
        while (resultIterator.hasNext()) {
            final T record = resultIterator.next();
            if (!matchThenNext(record)) {
                duplicateRead.add(record);
            } else {
                recordCounter++;
            }

            if (limit != null && recordCounter >= limit) {
                break;
            }
        }
        if (limit == null && !hasReachedEnd()) {
            failWithMessage(
                    generateMismatchDescription(
                            String.format(
                                    "Expected to have at least %d records in result, but only received %d records",
                                    recordsFromSplits.stream()
                                            .mapToInt(
                                                    recordsFromSplit ->
                                                            recordsFromSplit.records.size())
                                            .sum(),
                                    recordCounter),
                            resultIterator));
        } else {
            confirmDuplicateRead(duplicateRead);
        }
    }

    private void compareWithExactlyOnceSemantic(
            Iterator<T> resultIterator, List<RecordsFromSplit<T>> expectedData) {
        int recordCounter = 0;
        while (resultIterator.hasNext()) {
            final T recordReceived = resultIterator.next();
            if (!matchThenNext(recordReceived)) {
                if (recordCounter >= totalNumRecords) {
                    failWithMessage(
                            generateMismatchDescription(
                                    String.format(
                                            "Expected to have exactly %d records in result, but received more records",
                                            expectedData.stream()
                                                    .mapToInt(
                                                            recordsFromSplit ->
                                                                    recordsFromSplit.records.size())
                                                    .sum()),
                                    resultIterator));
                } else {
                    failWithMessage(
                            generateMismatchDescription(
                                    String.format(
                                            "Unexpected record '%s' at position %d(count of matched records)",
                                            recordReceived, recordCounter),
                                    resultIterator));
                }
            }
            recordCounter++;
            if (limit != null && recordCounter >= limit) {
                break;
            }
        }
        if (limit == null && !hasReachedEnd()) {
            failWithMessage(
                    generateMismatchDescription(
                            String.format(
                                    "Expected to have exactly %d records in result, but only received %d records",
                                    expectedData.stream()
                                            .mapToInt(
                                                    recordsFromSplit ->
                                                            recordsFromSplit.records.size())
                                            .sum(),
                                    recordCounter),
                            resultIterator));
        }
    }

    private void confirmDuplicateRead(List<T> duplicateRead) {
        for (T record : duplicateRead) {
            boolean found = false;
            for (RecordsFromSplit<T> recordsFromSplit : expectedRecordsFromSplits) {
                if (recordsFromSplit.records.contains(record)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                failWithMessage(String.format("Unexpected duplicate record '%s'", record));
            }
        }
    }

    /**
     * Check if any pointing data is identical to the record from the stream, and move the pointer
     * to next record if matched.
     *
     * @param record Record from stream
     */
    private boolean matchThenNext(T record) {
        for (RecordsFromSplit<T> recordsFromSplit : expectedRecordsFromSplits) {
            if (!recordsFromSplit.hasNext()) {
                continue;
            }
            if (record.equals(recordsFromSplit.current())) {
                recordsFromSplit.forward();
                return true;
            }
        }
        return false;
    }

    /**
     * Whether all pointers have reached the end of lists.
     *
     * @return True if all pointers have reached the end.
     */
    private boolean hasReachedEnd() {
        for (RecordsFromSplit<T> recordsFromSplit : expectedRecordsFromSplits) {
            if (recordsFromSplit.hasNext()) {
                return false;
            }
        }
        return true;
    }

    private String generateMismatchDescription(String reason, Iterator<T> resultIterator) {
        final StringBuilder sb = new StringBuilder();
        sb.append(reason).append("\n");
        sb.append("Current progress of multiple split test data validation:\n");
        int splitCounter = 0;
        for (RecordsFromSplit<T> recordsFromSplit : expectedRecordsFromSplits) {
            sb.append(
                    String.format(
                            "Split %d (%d/%d): \n",
                            splitCounter++,
                            recordsFromSplit.offset,
                            recordsFromSplit.records.size()));
            for (int recordIndex = 0;
                    recordIndex < recordsFromSplit.records.size();
                    recordIndex++) {
                sb.append(recordsFromSplit.records.get(recordIndex));
                if (recordIndex == recordsFromSplit.offset) {
                    sb.append("\t<---- failed to compare with the current record");
                }
                sb.append("\n");
            }
        }
        // Modify the following info building to avoid the test being stuck.
        if (resultIterator.hasNext()) {
            sb.append("Remaining received elements after the unexpected one: xxx\n");
            //            while (resultIterator.hasNext()) {
            //                sb.append(resultIterator.next()).append("\n");
            //            }
        }
        return sb.toString();
    }

    private static class RecordsFromSplit<T> {
        private int offset = 0;
        private final List<T> records;

        public RecordsFromSplit(List<T> records) {
            this.records = records;
        }

        public T current() {
            if (!hasNext()) {
                return null;
            }
            return records.get(offset);
        }

        public void forward() {
            ++offset;
        }

        public boolean hasNext() {
            return offset < records.size();
        }
    }
}
