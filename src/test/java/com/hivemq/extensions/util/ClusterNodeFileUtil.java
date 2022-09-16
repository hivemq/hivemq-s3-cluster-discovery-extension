package com.hivemq.extensions.util;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extensions.config.ClusterNodeFile;

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
        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
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
        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    public static @NotNull String createClusterNodeFileStringTooShort(
            final @NotNull String version,
            final @NotNull String timeInMillis,
            final @NotNull String nodeId,
            final @NotNull String host,
            final @NotNull String port) {
        final String content =
                version + ClusterNodeFile.CONTENT_SEPARATOR + timeInMillis + ClusterNodeFile.CONTENT_SEPARATOR;
        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }
}
