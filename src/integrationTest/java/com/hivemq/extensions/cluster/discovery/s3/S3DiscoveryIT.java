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

import com.hivemq.extensions.cluster.discovery.s3.util.MetricsUtil;
import com.hivemq.extensions.cluster.discovery.s3.util.TestConfigFile;
import io.github.sgtsilvio.gradle.oci.junit.jupiter.OciImages;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers
class S3DiscoveryIT {

    private static final @NotNull String BUCKET_NAME = "hivemq";

    private final @NotNull Network network = Network.newNetwork();

    @Container
    private final @NotNull LocalStackContainer localstack =
            new LocalStackContainer(OciImages.getImageName("localstack/localstack")).withServices(S3)
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    private @NotNull HiveMQContainer firstNode;
    private @NotNull HiveMQContainer secondNode;

    @BeforeEach
    void setUp() throws IOException {
        createS3Environment();
        final String s3Config = TestConfigFile.builder()
                .setS3BucketName(BUCKET_NAME)
                .setS3Endpoint("http://localstack:4566")
                .setS3EndpointRegion(localstack.getRegion())
                .build();

        firstNode =
                new HiveMQContainer(OciImages.getImageName("hivemq/extensions/hivemq-s3-cluster-discovery-extension")
                        .asCompatibleSubstituteFor("hivemq/hivemq4")) //
                        .withLogLevel(Level.DEBUG)
                        .withNetwork(network)
                        .withHiveMQConfig(MountableFile.forClasspathResource("hivemq-config.xml"))
                        .withExposedPorts(9399)
                        .withEnv("AWS_ACCESS_KEY_ID", localstack.getAccessKey())
                        .withEnv("AWS_SECRET_ACCESS_KEY", localstack.getSecretKey())
                        .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                        .withCopyToContainer(Transferable.of(s3Config),
                                "/opt/hivemq/extensions/hivemq-s3-cluster-discovery-extension/s3discovery.properties");

        secondNode =
                new HiveMQContainer(OciImages.getImageName("hivemq/extensions/hivemq-s3-cluster-discovery-extension")
                        .asCompatibleSubstituteFor("hivemq/hivemq4")) //
                        .withLogLevel(Level.DEBUG)
                        .withNetwork(network)
                        .withHiveMQConfig(MountableFile.forClasspathResource("hivemq-config.xml"))
                        .withExposedPorts(9399)
                        .withEnv("AWS_ACCESS_KEY_ID", localstack.getAccessKey())
                        .withEnv("AWS_SECRET_ACCESS_KEY", localstack.getSecretKey())
                        .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                        .withCopyToContainer(Transferable.of(s3Config),
                                "/opt/hivemq/extensions/hivemq-s3-cluster-discovery-extension/s3discovery.properties");
    }

    @AfterEach
    void tearDown() {
        localstack.stop();
        firstNode.stop();
        secondNode.stop();
        network.close();
    }

    @Test
    void cluster_singleNode() throws IOException {
        firstNode.start();
        final Map<String, Float> metrics = MetricsUtil.getMetrics(firstNode);
        assertEquals(1, metrics.get(MetricsUtil.SUCCESS_METRIC));
        assertEquals(0, metrics.get(MetricsUtil.FAILURE_METRIC));
        assertEquals(1, metrics.get(MetricsUtil.IP_COUNT_METRIC));
    }

    @Test
    void cluster_twoNode() throws IOException {
        firstNode.start();
        secondNode.start();
        final Map<String, Float> secondNodeMetrics = MetricsUtil.getMetrics(secondNode);
        assertEquals(1, secondNodeMetrics.get(MetricsUtil.SUCCESS_METRIC));
        assertEquals(0, secondNodeMetrics.get(MetricsUtil.FAILURE_METRIC));
        assertEquals(2, secondNodeMetrics.get(MetricsUtil.IP_COUNT_METRIC));
    }


    private void createS3Environment() {
        try (final S3Client s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(),
                        localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion()))
                .build()) {
            s3.createBucket(builder -> builder.bucket(BUCKET_NAME).build());
        }
    }
}
