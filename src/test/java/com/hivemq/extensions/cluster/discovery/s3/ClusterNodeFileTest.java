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

import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.extensions.cluster.discovery.s3.util.ClusterNodeFileUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterNodeFileTest {

    private final @NotNull ClusterNodeAddress clusterNodeAddress = new ClusterNodeAddress("127.0.0.1", 7800);
    private final @NotNull String nodeId = "ABCD12";

    @Test
    void test_cluster_node_file_successful_create() {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertThat(clusterNodeFile).isNotNull();
    }

    @Test
    void test_cluster_node_file_successful_get_node_address() {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertThat(clusterNodeFile.getClusterNodeAddress()).isNotNull();
    }

    @Test
    void test_cluster_node_file_successful_get_cluster_id() {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertThat(clusterNodeFile.getClusterId()).isNotNull();
    }

    @Test
    void test_cluster_node_file_equals() {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        final var clusterNodeFileString = clusterNodeFile.toString();
        final var newClusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(newClusterNodeFile).isNotNull();
        assertThat(newClusterNodeFile.toString()).isEqualTo(clusterNodeFile.toString());
    }

    @Test
    void test_cluster_node_file_not_equal() {
        final var clusterNodeFile1 = new ClusterNodeFile(nodeId + 1, clusterNodeAddress);
        final var clusterNodeFile2 = new ClusterNodeFile(nodeId + 2, clusterNodeAddress);
        assertThat(clusterNodeFile2.toString()).isNotEqualTo(clusterNodeFile1.toString());
    }

    @Test
    void test_cluster_node_file_nodeId_null() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new ClusterNodeFile(null,
                clusterNodeAddress)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void test_cluster_node_file_nodeId_blank() {
        assertThatThrownBy(() -> new ClusterNodeFile(" ",
                clusterNodeAddress)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_cluster_node_file_cluster_node_address_null() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> new ClusterNodeFile(nodeId, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void test_cluster_node_file_expiration_deactivated() {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertThat(clusterNodeFile.isExpired(0)).isFalse();
    }

    @Test
    void test_cluster_node_file_expired() throws Exception {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        TimeUnit.SECONDS.sleep(2);
        assertThat(clusterNodeFile.isExpired(1)).isTrue();
    }

    @Test
    void test_cluster_node_file_not_expired() {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertThat(clusterNodeFile.isExpired(1)).isFalse();
    }

    @Test
    void test_cluster_node_file_not_expired_sleep() throws Exception {
        final var clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        TimeUnit.SECONDS.sleep(1);
        assertThat(clusterNodeFile.isExpired(2)).isFalse();
    }

    @Test
    void test_parseClusterNodeFile_success() {
        final var clusterNodeFile1 = new ClusterNodeFile(nodeId, clusterNodeAddress);
        final var clusterNodeFile1String = clusterNodeFile1.toString();
        final var clusterNodeFile2 = ClusterNodeFile.parseClusterNodeFile(clusterNodeFile1String);
        assertThat(clusterNodeFile2).isNotNull();
        assertThat(clusterNodeFile2.toString()).isEqualTo(clusterNodeFile1.toString());
    }

    @Test
    void test_parseClusterNodeFile_false_version() {
        final var clusterNodeFileString = ClusterNodeFileUtil.createClusterNodeFileString("3",
                Long.toString(System.currentTimeMillis()),
                nodeId,
                clusterNodeAddress.getHost(),
                Integer.toString(clusterNodeAddress.getPort()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_false_charset() {
        final var clusterNodeFileString = new String("abcd".getBytes(), StandardCharsets.UTF_16);
        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_version_empty() {
        final var clusterNodeFileString = ClusterNodeFileUtil.createClusterNodeFileString("",
                Long.toString(System.currentTimeMillis()),
                nodeId,
                clusterNodeAddress.getHost(),
                Integer.toString(clusterNodeAddress.getPort()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_node_id_empty() {
        final var clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        "",
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_host_empty() {
        final var clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        "",
                        Integer.toString(clusterNodeAddress.getPort()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_port_not_number() {
        final var clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        "abcd");

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_creation_time_not_number() {
        final var clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        "abcd",
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_too_short() {
        final var clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileStringTooShort(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_too_long() {
        final var clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileStringTooLong(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final var clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertThat(clusterNodeFile).isNull();
    }

    @Test
    void test_parseClusterNodeFile_null() {
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> ClusterNodeFile.parseClusterNodeFile(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void test_parseClusterNodeFile_blank() {
        assertThatThrownBy(() -> ClusterNodeFile.parseClusterNodeFile("  ")).isInstanceOf(IllegalArgumentException.class);
    }
}
