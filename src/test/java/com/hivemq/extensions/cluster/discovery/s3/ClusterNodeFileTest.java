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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterNodeFileTest {

    private final @NotNull ClusterNodeAddress clusterNodeAddress = new ClusterNodeAddress("127.0.0.1", 7800);
    private final @NotNull String nodeId = "ABCD12";

    @Test
    void test_cluster_node_file_successful_create() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertNotNull(clusterNodeFile);
    }

    @Test
    void test_cluster_node_file_successful_get_node_address() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertNotNull(clusterNodeFile.getClusterNodeAddress());
    }

    @Test
    void test_cluster_node_file_successful_get_cluster_id() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertNotNull(clusterNodeFile.getClusterId());
    }

    @Test
    void test_cluster_node_file_equals() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        final String clusterNodeFileString = clusterNodeFile.toString();
        final ClusterNodeFile newClusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNotNull(newClusterNodeFile);
        assertEquals(clusterNodeFile.toString(), newClusterNodeFile.toString());
    }

    @Test
    void test_cluster_node_file_not_equal() {
        final ClusterNodeFile clusterNodeFile1 = new ClusterNodeFile(nodeId + 1, clusterNodeAddress);
        final ClusterNodeFile clusterNodeFile2 = new ClusterNodeFile(nodeId + 2, clusterNodeAddress);
        assertNotEquals(clusterNodeFile1.toString(), clusterNodeFile2.toString());
    }

    @Test
    void test_cluster_node_file_nodeId_null() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new ClusterNodeFile(null, clusterNodeAddress));
    }


    @Test
    void test_cluster_node_file_nodeId_blank() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterNodeFile(" ", clusterNodeAddress));
    }

    @Test
    void test_cluster_node_file_cluster_node_address_null() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new ClusterNodeFile(nodeId, null));
    }

    @Test
    void test_cluster_node_file_expiration_deactivated() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertFalse(clusterNodeFile.isExpired(0));
    }

    @Test
    void test_cluster_node_file_expired() throws Exception {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        TimeUnit.SECONDS.sleep(2);
        assertTrue(clusterNodeFile.isExpired(1));
    }

    @Test
    void test_cluster_node_file_not_expired() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        assertFalse(clusterNodeFile.isExpired(1));
    }

    @Test
    void test_cluster_node_file_not_expired_sleep() throws Exception {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        TimeUnit.SECONDS.sleep(1);
        assertFalse(clusterNodeFile.isExpired(2));
    }

    @Test
    void test_parseClusterNodeFile_success() {
        final ClusterNodeFile clusterNodeFile1 = new ClusterNodeFile(nodeId, clusterNodeAddress);
        final String clusterNodeFile1String = clusterNodeFile1.toString();
        final ClusterNodeFile clusterNodeFile2 = ClusterNodeFile.parseClusterNodeFile(clusterNodeFile1String);
        assertNotNull(clusterNodeFile2);
        assertEquals(clusterNodeFile1.toString(), clusterNodeFile2.toString());
    }

    @Test
    void test_parseClusterNodeFile_false_version() {
        final String clusterNodeFileString = ClusterNodeFileUtil.createClusterNodeFileString("3",
                Long.toString(System.currentTimeMillis()),
                nodeId,
                clusterNodeAddress.getHost(),
                Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_false_charset() {
        final String clusterNodeFileString = new String("abcd".getBytes(), StandardCharsets.UTF_16);
        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_version_empty() {
        final String clusterNodeFileString = ClusterNodeFileUtil.createClusterNodeFileString("",
                Long.toString(System.currentTimeMillis()),
                nodeId,
                clusterNodeAddress.getHost(),
                Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_node_id_empty() {
        final String clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        "",
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_host_empty() {
        final String clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        "",
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_port_not_number() {
        final String clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        "abcd");

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_creation_time_not_number() {
        final String clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileString(ClusterNodeFile.CONTENT_VERSION,
                        "abcd",
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_too_short() {
        final String clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileStringTooShort(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_too_long() {
        final String clusterNodeFileString =
                ClusterNodeFileUtil.createClusterNodeFileStringTooLong(ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        assertNull(clusterNodeFile);
    }

    @Test
    void test_parseClusterNodeFile_null() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> ClusterNodeFile.parseClusterNodeFile(null));
    }

    @Test
    void test_parseClusterNodeFile_blank() {
        assertThrows(IllegalArgumentException.class, () -> ClusterNodeFile.parseClusterNodeFile("  "));
    }
}
