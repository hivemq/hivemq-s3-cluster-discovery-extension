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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class S3DiscoveryMetricsTest {

    private @NotNull MetricRegistry metricRegistry;
    private @NotNull S3DiscoveryMetrics metrics;

    @BeforeEach
    void setUp() {
        metricRegistry = new MetricRegistry();
        metrics = new S3DiscoveryMetrics(metricRegistry);
    }

    @Test
    void test_getQuerySuccessCount() {
        final Counter counter = metrics.getQuerySuccessCount();
        counter.inc();
        final String name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "query.success.count";
        final Counter counterFromRegistry = metricRegistry.counter(name);
        assertEquals(counter.getCount(), counterFromRegistry.getCount());
    }

    @Test
    void test_getQueryFailedCount() {
        final Counter counter = metrics.getQueryFailedCount();
        counter.inc();
        final String name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "query.failed.count";
        final Counter counterFromRegistry = metricRegistry.counter(name);
        assertEquals(counter.getCount(), counterFromRegistry.getCount());
    }

    @Test
    void test_registerAddressCountGauge() {
        final AtomicInteger addressesCount = new AtomicInteger(1);
        metrics.registerAddressCountGauge(addressesCount::get);

        final String name = ExtensionConstants.EXTENSION_METRIC_PREFIX + ".resolved-addresses";
        final Gauge<?> gauge = metricRegistry.getGauges().get(name);

        assertNotNull(gauge);
        assertEquals(1, gauge.getValue());

        addressesCount.set(3);
        assertEquals(3, gauge.getValue());
    }
}
