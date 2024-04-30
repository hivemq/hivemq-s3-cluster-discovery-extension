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

package com.hivemq.extensions.cluster.discovery.s3.util;

import com.hivemq.extensions.cluster.discovery.s3.ClusterNodeFile;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ClusterNodeFileUtil {

    public static @NotNull String createClusterNodeFileString(
            final @NotNull String version,
            final @NotNull String timeInMillis,
            final @NotNull String nodeId,
            final @NotNull String host,
            final @NotNull String port) {
        final String content = version +
                ClusterNodeFile.CONTENT_SEPARATOR +
                timeInMillis +
                ClusterNodeFile.CONTENT_SEPARATOR +
                nodeId +
                ClusterNodeFile.CONTENT_SEPARATOR +
                host +
                ClusterNodeFile.CONTENT_SEPARATOR +
                port +
                ClusterNodeFile.CONTENT_SEPARATOR;
        return Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    public static @NotNull String createClusterNodeFileStringTooLong(
            final @NotNull String version,
            final @NotNull String timeInMillis,
            final @NotNull String nodeId,
            final @NotNull String host,
            final @NotNull String port) {
        final String content = version +
                ClusterNodeFile.CONTENT_SEPARATOR +
                timeInMillis +
                ClusterNodeFile.CONTENT_SEPARATOR +
                nodeId +
                ClusterNodeFile.CONTENT_SEPARATOR +
                host +
                ClusterNodeFile.CONTENT_SEPARATOR +
                port +
                ClusterNodeFile.CONTENT_SEPARATOR +
                port +
                ClusterNodeFile.CONTENT_SEPARATOR;
        return Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    public static @NotNull String createClusterNodeFileStringTooShort(
            final @NotNull String version, final @NotNull String timeInMillis) {
        final String content =
                version + ClusterNodeFile.CONTENT_SEPARATOR + timeInMillis + ClusterNodeFile.CONTENT_SEPARATOR;
        return Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }
}
