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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.hivemq.extension.sdk.api.annotations.NotNull;

import static com.hivemq.extensions.ExtensionConstants.EXTENSION_METRIC_PREFIX;


/**
 * @author Lukas Brand
 */
public class ExtensionMetrics {

    private final @NotNull MetricRegistry metricRegistry;
    private final @NotNull Counter connectCounter;
    private final @NotNull Counter failedConnectCounter;

    public ExtensionMetrics(final @NotNull MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.connectCounter =
                metricRegistry.counter(MetricRegistry.name(EXTENSION_METRIC_PREFIX, "query.success.count"));
        this.failedConnectCounter =
                metricRegistry.counter(MetricRegistry.name(EXTENSION_METRIC_PREFIX, "query.failed.count"));
    }

    public @NotNull Counter getResolutionRequestCounter() {
        return connectCounter;
    }

    public @NotNull Counter getResolutionRequestFailedCounter() {
        return failedConnectCounter;
    }

    public void registerAddressCountGauge(final @NotNull Gauge<Integer> supplier) {
        metricRegistry.gauge(MetricRegistry.name(EXTENSION_METRIC_PREFIX, "resolved-addresses"), () -> supplier);
    }

    public void stop() {
        metricRegistry.removeMatching((name, metric) -> name.startsWith(EXTENSION_METRIC_PREFIX + ".") &&
                metric instanceof Gauge);
    }
}
