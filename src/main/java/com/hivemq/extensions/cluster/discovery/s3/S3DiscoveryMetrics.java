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

package com.hivemq.extensions.cluster.discovery.s3;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.jetbrains.annotations.NotNull;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_METRIC_PREFIX;

/**
 * @author Lukas Brand
 */
class S3DiscoveryMetrics {

    private final @NotNull MetricRegistry metricRegistry;
    private final @NotNull Counter querySuccessCount;
    private final @NotNull Counter queryFailedCount;

    S3DiscoveryMetrics(final @NotNull MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        querySuccessCount = metricRegistry.counter(MetricRegistry.name(EXTENSION_METRIC_PREFIX, "query.success.count"));
        queryFailedCount = metricRegistry.counter(MetricRegistry.name(EXTENSION_METRIC_PREFIX, "query.failed.count"));
    }

    @NotNull Counter getQuerySuccessCount() {
        return querySuccessCount;
    }

    @NotNull Counter getQueryFailedCount() {
        return queryFailedCount;
    }

    void registerAddressCountGauge(final @NotNull Gauge<Integer> supplier) {
        metricRegistry.gauge(MetricRegistry.name(EXTENSION_METRIC_PREFIX, "resolved-addresses"), () -> supplier);
    }

    void stop() {
        metricRegistry.removeMatching((name, metric) -> name.startsWith(EXTENSION_METRIC_PREFIX + ".") &&
                metric instanceof Gauge);
    }
}
