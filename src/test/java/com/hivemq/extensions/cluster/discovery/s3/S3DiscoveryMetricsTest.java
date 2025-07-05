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

import com.codahale.metrics.MetricRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
        final var counter = metrics.getQuerySuccessCount();
        counter.inc();
        final var name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "query.success.count";
        final var counterFromRegistry = metricRegistry.counter(name);
        assertThat(counterFromRegistry.getCount()).isEqualTo(counter.getCount());
    }

    @Test
    void test_getQueryFailedCount() {
        final var counter = metrics.getQueryFailedCount();
        counter.inc();
        final var name = ExtensionConstants.EXTENSION_METRIC_PREFIX + "." + "query.failed.count";
        final var counterFromRegistry = metricRegistry.counter(name);
        assertThat(counterFromRegistry.getCount()).isEqualTo(counter.getCount());
    }

    @Test
    void test_registerAddressCountGauge() {
        final var addressesCount = new AtomicInteger(1);
        metrics.registerAddressCountGauge(addressesCount::get);

        final var name = ExtensionConstants.EXTENSION_METRIC_PREFIX + ".resolved-addresses";
        final var gauge = metricRegistry.getGauges().get(name);
        assertThat(gauge).isNotNull();
        assertThat(gauge.getValue()).isEqualTo(1);

        addressesCount.set(3);
        assertThat(gauge.getValue()).isEqualTo(3);
    }
}
