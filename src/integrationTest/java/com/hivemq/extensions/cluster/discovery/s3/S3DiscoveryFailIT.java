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
import org.testcontainers.utility.MountableFile;

import static com.hivemq.extensions.cluster.discovery.s3.util.MetricsUtil.FAILURE_METRIC;
import static com.hivemq.extensions.cluster.discovery.s3.util.MetricsUtil.IP_COUNT_METRIC;
import static com.hivemq.extensions.cluster.discovery.s3.util.MetricsUtil.SUCCESS_METRIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

class S3DiscoveryFailIT {

    private static final @NotNull String BUCKET_NAME = "hivemq";

    private final @NotNull Network network = Network.newNetwork();

    private final @NotNull LocalStackContainer localstack =
            new LocalStackContainer(OciImages.getImageName("localstack/localstack")).withServices(S3)
                    .withNetwork(network)
                    .withNetworkAliases("localstack");

    private @NotNull HiveMQContainer firstNode;

    @BeforeEach
    void setUp() throws Exception {
        final var s3Config = TestConfigFile.builder()
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
    }

    @AfterEach
    void tearDown() {
        localstack.stop();
        firstNode.stop();
        network.close();
    }

    @Test
    void endpoint_not_working() throws Exception {
        firstNode.start();
        final var metrics = MetricsUtil.getMetrics(firstNode);
        assertThat(metrics.get(SUCCESS_METRIC)).isEqualTo(0);
        assertThat(metrics.get(FAILURE_METRIC)).isEqualTo(1);
        assertThat(metrics.get(IP_COUNT_METRIC)).isEqualTo(0);
    }

    @Test
    void bucket_not_created() throws Exception {
        localstack.start();
        firstNode.start();
        final var metrics = MetricsUtil.getMetrics(firstNode);
        assertThat(metrics.get(SUCCESS_METRIC)).isEqualTo(0);
        assertThat(metrics.get(FAILURE_METRIC)).isEqualTo(1);
        assertThat(metrics.get(IP_COUNT_METRIC)).isEqualTo(0);
    }
}
