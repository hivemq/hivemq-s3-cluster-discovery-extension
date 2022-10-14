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

package com.hivemq.extensions;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extensions.config.TestS3Config;
import com.hivemq.extensions.metrics.TestS3Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.hivemq.extensions.metrics.TestS3Metrics.FAILURE_METRIC;
import static com.hivemq.extensions.metrics.TestS3Metrics.IP_COUNT_METRIC;
import static com.hivemq.extensions.metrics.TestS3Metrics.SUCCESS_METRIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

class S3DiscoveryFailIT {

    private static final @NotNull String BUCKET_NAME = "hivemq";

    private final @NotNull Network network = Network.newNetwork();

    private final @NotNull LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack").withTag("latest")).withServices(S3)
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    private @NotNull HiveMQContainer firstNode;

    @BeforeEach
    void setUp(@TempDir final @NotNull Path tempDir) throws IOException {
        final MountableFile configFile = TestS3Config.builder(tempDir)
                .setS3BucketName(BUCKET_NAME)
                .setS3Endpoint("http://localstack:4566")
                .setS3EndpointRegion(localstack.getRegion())
                .build();

        firstNode =
                new HiveMQContainer(DockerImageName.parse("hivemq/hivemq4").withTag("latest")).withLogLevel(Level.DEBUG)
                        .withNetwork(network)
                        .withoutPrepackagedExtensions()
                        .withHiveMQConfig(MountableFile.forClasspathResource("hivemq-config.xml"))
                        .withExtension(MountableFile.forClasspathResource("hivemq-prometheus-extension"))
                        .withExposedPorts(9399)
                        .withExtension(MountableFile.forClasspathResource("hivemq-s3-cluster-discovery-extension"))
                        .withEnv("AWS_ACCESS_KEY_ID", localstack.getAccessKey())
                        .withEnv("AWS_SECRET_ACCESS_KEY", localstack.getAccessKey())
                        .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                        .withFileInExtensionHomeFolder(configFile,
                                "hivemq-s3-cluster-discovery-extension",
                                "s3discovery.properties");
    }

    @Test
    void endpoint_not_working() throws IOException {
        firstNode.start();
        final Map<String, Float> metrics = TestS3Metrics.getInstance().getMetrics(firstNode);
        assertEquals(0, metrics.get(SUCCESS_METRIC));
        assertEquals(1, metrics.get(FAILURE_METRIC));
        assertEquals(0, metrics.get(IP_COUNT_METRIC));
    }

    @Test
    void bucket_not_created() throws IOException {
        localstack.start();
        firstNode.start();
        final Map<String, Float> metrics = TestS3Metrics.getInstance().getMetrics(firstNode);
        assertEquals(0, metrics.get(SUCCESS_METRIC));
        assertEquals(1, metrics.get(FAILURE_METRIC));
        assertEquals(0, metrics.get(IP_COUNT_METRIC));
    }
}
