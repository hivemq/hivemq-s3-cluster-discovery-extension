package com.hivemq.plugin.config;

import com.hivemq.plugin.api.services.cluster.parameter.ClusterNodeAddress;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ClusterNodeFileTest {

    private final ClusterNodeAddress clusterNodeAddress = new ClusterNodeAddress("127.0.0.1", 7800);
    private final String nodeId = "ABCD12";

    @Test
    public void test_cluster_node_file_successful_create() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        Assert.assertNotNull(clusterNodeFile);
    }

    @Test
    public void test_cluster_node_file_equals() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        final String clusterNodeFileString = clusterNodeFile.toString();
        final ClusterNodeFile newClusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertTrue(clusterNodeFile.toString().contentEquals(newClusterNodeFile.toString()));
    }

    @Test
    public void test_cluster_node_file_not_equal() {
        final ClusterNodeFile clusterNodeFile1 = new ClusterNodeFile(nodeId + 1, clusterNodeAddress);
        final ClusterNodeFile clusterNodeFile2 = new ClusterNodeFile(nodeId + 2, clusterNodeAddress);
        Assert.assertFalse(clusterNodeFile1.toString().contentEquals(clusterNodeFile2.toString()));
    }

    @Test(expected = NullPointerException.class)
    public void test_cluster_node_file_nodeId_null() {
        new ClusterNodeFile(null, clusterNodeAddress);
    }


    @Test(expected = IllegalArgumentException.class)
    public void test_cluster_node_file_nodeId_blank() {
        new ClusterNodeFile(" ", clusterNodeAddress);
    }

    @Test(expected = NullPointerException.class)
    public void test_cluster_node_file_cluster_node_address_null() {
        new ClusterNodeFile(nodeId, null);
    }

    @Test
    public void test_cluster_node_file_expiration_deactivated() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        Assert.assertFalse(clusterNodeFile.isExpired(0));
    }

    @Test
    public void test_cluster_node_file_expired() throws Exception {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        TimeUnit.SECONDS.sleep(2);
        Assert.assertTrue(clusterNodeFile.isExpired(1));
    }

    @Test
    public void test_cluster_node_file_not_expired() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        Assert.assertFalse(clusterNodeFile.isExpired(1));
    }

    @Test
    public void test_cluster_node_file_not_expired_sleep() throws Exception {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        TimeUnit.SECONDS.sleep(1);
        Assert.assertFalse(clusterNodeFile.isExpired(2));
    }
}