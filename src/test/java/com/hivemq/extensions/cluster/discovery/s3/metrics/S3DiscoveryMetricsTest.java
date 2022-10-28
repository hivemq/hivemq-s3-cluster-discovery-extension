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

package com.hivemq.extensions.cluster.discovery.s3.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3DiscoveryMetricsTest {

    private @NotNull MetricRegistry metricRegistry;
    private @NotNull ExtensionMetrics metrics;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        metrics = new ExtensionMetrics(metricRegistry);
    }

    @Test
    void test_resolutionRequestCounter() {
        final Counter counter = metrics.getResolutionRequestCounter();
        counter.inc();
        final String name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "query.success.count";
        final Counter counterFromRegistry = metricRegistry.counter(name);
        assertEquals(counter.getCount(), counterFromRegistry.getCount());
    }

    @Test
    void test_resolutionRequestCounterFailed() {
        final Counter counter = metrics.getResolutionRequestFailedCounter();
        counter.inc();
        final String name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "query.failed.count";
        final Counter counterFromRegistry = metricRegistry.counter(name);
        assertEquals(counter.getCount(), counterFromRegistry.getCount());
    }

    @Test
    void test_registerAddressCountGauge() {
        final List<Integer> addresses = new ArrayList<>(List.of(1));
        final AtomicInteger addressesCount = new AtomicInteger(0);

        addressesCount.set(addresses.size());

        metrics.registerAddressCountGauge(addressesCount::get);

        final String name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "resolved-addresses";
        final Gauge<?> gauge = metricRegistry.getGauges().get(name);

        assertEquals(addresses.size(), gauge.getValue());

        addresses.add(2);
        addresses.add(3);
        addressesCount.set(addresses.size());

        assertEquals(addresses.size(), gauge.getValue());
    }
}
