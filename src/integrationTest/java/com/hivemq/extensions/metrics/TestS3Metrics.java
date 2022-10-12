/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.metrics;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extensions.S3DiscoveryIT;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.hivemq.HiveMQContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestS3Metrics {

    private static final @NotNull TestS3Metrics testS3Metrics = new TestS3Metrics();
    private static final @NotNull Logger log = LoggerFactory.getLogger(S3DiscoveryIT.class);

    public static final @NotNull String SUCCESS_METRIC =
            "com_hivemq_extensions_s3_cluster_discovery_query_success_count";
    public static final @NotNull String FAILURE_METRIC =
            "com_hivemq_extensions_s3_cluster_discovery_query_failed_count";
    public static final @NotNull String IP_COUNT_METRIC =
            "com_hivemq_extensions_s3_cluster_discovery_resolved_addresses";

    private TestS3Metrics() {
    }

    public static @NotNull TestS3Metrics getInstance() {
        return testS3Metrics;
    }

    public @NotNull Map<String, Float> getMetrics(final @NotNull HiveMQContainer node) throws IOException {
        final OkHttpClient client = new OkHttpClient();
        final Request request1 =
                new Request.Builder().url("http://" + node.getHost() + ":" + node.getMappedPort(9399) + "/metrics")
                        .build();

        final Response response1 = client.newCall(request1).execute();
        assertNotNull(response1.body());
        final String string = response1.body().string();

        return parseMetrics(string, Set.of(SUCCESS_METRIC, FAILURE_METRIC, IP_COUNT_METRIC));
    }

    private @NotNull Map<String, Float> parseMetrics(
            final @NotNull String metricsDump, final @NotNull Set<String> metrics) {
        return metricsDump.lines()
                .filter(s -> !s.startsWith("#"))
                .map(s -> s.split(" "))
                .filter(splits -> metrics.contains(splits[0]))
                .peek(strings -> log.info(Arrays.toString(strings)))
                .map(splits -> Map.entry(splits[0], Float.parseFloat(splits[1])))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Float::max));
    }
}
