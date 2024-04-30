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

public class ExtensionConstants {

    public static final @NotNull String EXTENSION_NAME = "HiveMQ S3 Cluster Discovery Extension";

    public static final @NotNull String EXTENSION_METRIC_PREFIX =
            MetricRegistry.name("com", "hivemq", "extensions", "cluster", "discovery", "s3");

    public static final @NotNull String EXTENSION_CONFIGURATION = "s3discovery.properties";

    private ExtensionConstants() {
    }
}
