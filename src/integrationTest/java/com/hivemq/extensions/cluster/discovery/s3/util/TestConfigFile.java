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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertNotNull;

public class TestConfigFile {

    private @NotNull String s3BucketRegion = "us-east-1";
    private @NotNull String s3BucketName = "hivemq";
    private @NotNull String filePrefix = "hivemq/cluster/nodes/";
    private @NotNull String fileExpiration = "360";
    private @NotNull String updateInterval = "180";
    private @NotNull String s3Endpoint = "s3.amazonaws.com";
    private boolean s3EndpointActive = false;
    private @NotNull String s3EndpointRegion = "";

    private TestConfigFile() {
    }

    public static @NotNull TestConfigFile builder() {
        return new TestConfigFile();
    }

    public @NotNull TestConfigFile setS3BucketRegion(final @NotNull String s3BucketRegion) {
        this.s3BucketRegion = s3BucketRegion;
        return this;
    }

    public @NotNull TestConfigFile setS3BucketName(final @NotNull String s3BucketName) {
        this.s3BucketName = s3BucketName;
        return this;
    }

    public @NotNull TestConfigFile setFilePrefix(final @NotNull String filePrefix) {
        this.filePrefix = filePrefix;
        return this;
    }

    public @NotNull TestConfigFile setFileExpiration(final @NotNull String fileExpiration) {
        this.fileExpiration = fileExpiration;
        return this;
    }

    public @NotNull TestConfigFile setUpdateInterval(final @NotNull String updateInterval) {
        this.updateInterval = updateInterval;
        return this;
    }

    public @NotNull TestConfigFile setS3Endpoint(final @NotNull String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public @NotNull TestConfigFile setS3EndpointRegion(final @NotNull String s3EndpointRegion) {
        this.s3EndpointActive = true;
        this.s3EndpointRegion = s3EndpointRegion;
        return this;
    }

    public @NotNull String build() throws IOException {
        final URL resource = getClass().getResource("/configurations/template-s3discovery.properties");
        assertNotNull(resource);

        String configTemplate = Files.readString(Path.of(resource.getPath()));
        configTemplate = configTemplate.replace("$BUCKET_REGION", s3BucketRegion);
        configTemplate = configTemplate.replace("$BUCKET_NAME", s3BucketName);
        configTemplate = configTemplate.replace("$FILE_PREFIX", filePrefix);
        configTemplate = configTemplate.replace("$FILE_EXPIRATION", fileExpiration);
        configTemplate = configTemplate.replace("$UPDATE_INTERVAL", updateInterval);
        configTemplate = configTemplate.replace("$S3_ENDPOINT", s3Endpoint);
        if (s3EndpointActive) {
            configTemplate = configTemplate.replace("$TOGGLE_ENDPOINT_REGION", "");
            configTemplate = configTemplate.replace("$S3_REGION_ENDPOINT", s3EndpointRegion);
        } else {
            configTemplate = configTemplate.replace("$TOGGLE_REGION_ENDPOINT", "# ");
        }
        return configTemplate;
    }
}
