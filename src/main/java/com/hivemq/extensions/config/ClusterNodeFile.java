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

package com.hivemq.extensions.config;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;

import java.util.Base64;
import java.util.regex.Pattern;

import static com.hivemq.extensions.util.Preconditions.checkArgument;
import static com.hivemq.extensions.util.Preconditions.checkNotNull;
import static com.hivemq.extensions.util.Preconditions.checkNotNullOrBlank;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class ClusterNodeFile {

    public static final @NotNull String CONTENT_VERSION = "4";
    public static final @NotNull String CONTENT_SEPARATOR = "||||";
    private static final @NotNull Pattern CONTENT_SEPARATOR_PATTERN = Pattern.compile("\\|\\|\\|\\|");

    private final @NotNull String clusterId;
    private final @NotNull ClusterNodeAddress clusterNodeAddress;
    private final long creationTimeInMillis;

    public ClusterNodeFile(final @NotNull String clusterId, final @NotNull ClusterNodeAddress clusterNodeAddress) {
        checkNotNullOrBlank(clusterId, "clusterId");
        checkNotNull(clusterNodeAddress, "clusterNodeAddress");
        this.clusterId = clusterId;
        this.clusterNodeAddress = clusterNodeAddress;
        creationTimeInMillis = System.currentTimeMillis();
    }

    private ClusterNodeFile(
            final @NotNull String clusterId,
            final @NotNull ClusterNodeAddress clusterNodeAddress,
            final long creationTimeInMillis) {
        checkNotNullOrBlank(clusterId, "clusterId");
        checkNotNull(clusterNodeAddress, "clusterNodeAddress");
        checkArgument(creationTimeInMillis > 0, "CreationTimeInMillis must not be zero or negative!");
        this.clusterId = clusterId;
        this.clusterNodeAddress = clusterNodeAddress;
        this.creationTimeInMillis = creationTimeInMillis;
    }

    public static @Nullable ClusterNodeFile parseClusterNodeFile(final @NotNull String fileContent) {
        checkNotNullOrBlank(fileContent, "fileContent");

        final String content;
        try {
            content = new String(Base64.getDecoder().decode(fileContent), UTF_8);
        } catch (final IllegalArgumentException ignored) {
            return null;
        }

        final String[] splitContent = CONTENT_SEPARATOR_PATTERN.split(content);
        if (splitContent.length != 5) {
            return null;
        }

        final String version = splitContent[0];
        if (!version.contentEquals(CONTENT_VERSION)) {
            return null;
        }

        final long creationTimeInMillis;
        try {
            creationTimeInMillis = Long.parseLong(splitContent[1]);
        } catch (final NumberFormatException ignored) {
            return null;
        }

        final String clusterId = splitContent[2];
        if (clusterId.length() < 1) {
            return null;
        }
        final String host = splitContent[3];
        if (host.length() < 1) {
            return null;
        }

        final int port;
        try {
            port = Integer.parseInt(splitContent[4]);
        } catch (final NumberFormatException ignored) {
            return null;
        }

        return new ClusterNodeFile(clusterId, new ClusterNodeAddress(host, port), creationTimeInMillis);
    }

    public @NotNull String getClusterId() {
        return clusterId;
    }

    public @NotNull ClusterNodeAddress getClusterNodeAddress() {
        return clusterNodeAddress;
    }

    public boolean isExpired(final long expirationInSeconds) {
        // 0 = deactivated
        if (expirationInSeconds == 0) {
            return false;
        }
        final long creationPlusExpirationInMillis = creationTimeInMillis + (expirationInSeconds * 1_000);
        return creationPlusExpirationInMillis < System.currentTimeMillis();
    }

    @Override
    public @NotNull String toString() {
        final String content = CONTENT_VERSION +
                CONTENT_SEPARATOR +
                creationTimeInMillis +
                CONTENT_SEPARATOR +
                clusterId +
                CONTENT_SEPARATOR +
                clusterNodeAddress.getHost() +
                CONTENT_SEPARATOR +
                clusterNodeAddress.getPort();
        return new String(Base64.getEncoder().encode(content.getBytes(UTF_8)), UTF_8);
    }
}
