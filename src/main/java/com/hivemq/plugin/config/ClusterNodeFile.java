/*
 * Copyright 2018 dc-square GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.plugin.config;

import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.api.annotations.Nullable;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterNodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class ClusterNodeFile {

    private static final Logger logger = LoggerFactory.getLogger(ClusterNodeFile.class);

    private static final String CONTENT_SEPARATOR = "||||";
    private static final String CONTENT_SEPARATOR_REGEX = "\\|\\|\\|\\|";
    private static final String CONTENT_VERSION = "2";

    private final String clusterId;
    private final ClusterNodeAddress clusterNodeAddress;
    private final String version;
    private final long creationTimeInMillis;

    public ClusterNodeFile(@NotNull final String clusterId, @NotNull final ClusterNodeAddress clusterNodeAddress) {
        this.clusterId = clusterId;
        this.clusterNodeAddress = clusterNodeAddress;
        this.creationTimeInMillis = System.currentTimeMillis();
        this.version = CONTENT_VERSION;
    }

    private ClusterNodeFile(@NotNull final String clusterId, @NotNull final ClusterNodeAddress clusterNodeAddress, final long creationTimeInMillis) {
        this.clusterId = clusterId;
        this.clusterNodeAddress = clusterNodeAddress;
        this.creationTimeInMillis = creationTimeInMillis;
        this.version = CONTENT_VERSION;
    }

    @Nullable
    public static ClusterNodeFile parseClusterNodeFile(@NotNull final String fileContent) {

        final String content;
        try {
            content = new String(Base64.getDecoder().decode(fileContent), StandardCharsets.UTF_8);
        } catch (final IllegalArgumentException ignored) {
            return null;
        }

        final String[] splitContent = content.split(CONTENT_SEPARATOR_REGEX);
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

    @NotNull
    public String getClusterId() {
        return clusterId;
    }

    @NotNull
    public ClusterNodeAddress getClusterNodeAddress() {
        return clusterNodeAddress;
    }

    @NotNull
    public String getVersion() {
        return version;
    }

    public long getCreationTimeInMillis() {
        return creationTimeInMillis;
    }

    public boolean isExpired(final long expirationInSeconds) {
        final long creationPlusExpirationInMillis = getCreationTimeInMillis() + (expirationInSeconds + 60_000);
        return creationPlusExpirationInMillis < System.currentTimeMillis();
    }

    @Override
    public String toString() {

        final String content =
                CONTENT_VERSION + CONTENT_SEPARATOR
                        + creationTimeInMillis + CONTENT_SEPARATOR
                        + clusterId + CONTENT_SEPARATOR
                        + clusterNodeAddress.getHost() + CONTENT_SEPARATOR
                        + clusterNodeAddress.getPort() + CONTENT_SEPARATOR;

        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }
}
