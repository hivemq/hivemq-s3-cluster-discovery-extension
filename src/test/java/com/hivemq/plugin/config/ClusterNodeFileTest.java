package com.hivemq.plugin.config;

import com.hivemq.plugin.api.services.cluster.parameter.ClusterNodeAddress;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
    public void test_cluster_node_file_successful_get_node_address() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        Assert.assertNotNull(clusterNodeFile.getClusterNodeAddress());
    }

    @Test
    public void test_cluster_node_file_successful_get_cluster_id() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile(nodeId, clusterNodeAddress);
        Assert.assertNotNull(clusterNodeFile.getClusterId());
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

    @Test
    public void test_parseClusterNodeFile_success() {
        final ClusterNodeFile clusterNodeFile1 = new ClusterNodeFile(nodeId, clusterNodeAddress);
        final String clusterNodeFile1String = clusterNodeFile1.toString();
        final ClusterNodeFile clusterNodeFile2 = ClusterNodeFile.parseClusterNodeFile(clusterNodeFile1String);
        Assert.assertTrue(clusterNodeFile1.toString().contentEquals(clusterNodeFile2.toString()));
    }

    @Test
    public void test_parseClusterNodeFile_false_version() {
        final String clusterNodeFileString =
                createClusterNodeFileString(
                        "3",
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_false_charset() {
        final String clusterNodeFileString = new String("abcd".getBytes(), StandardCharsets.UTF_16);
        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_version_empty() {
        final String clusterNodeFileString =
                createClusterNodeFileString(
                        "",
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_node_id_empty() {
        final String clusterNodeFileString =
                createClusterNodeFileString(
                        ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        "",
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_host_empty() {
        final String clusterNodeFileString =
                createClusterNodeFileString(
                        ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        "",
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_port_not_number() {
        final String clusterNodeFileString =
                createClusterNodeFileString(
                        ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        "abcd");

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_creation_time_not_number() {
        final String clusterNodeFileString =
                createClusterNodeFileString(
                        ClusterNodeFile.CONTENT_VERSION,
                        "abcd",
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_too_short() {
        final String clusterNodeFileString =
                createClusterNodeFileStringTooShort(
                        ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test
    public void test_parseClusterNodeFile_too_long() {
        final String clusterNodeFileString =
                createClusterNodeFileStringTooLong(
                        ClusterNodeFile.CONTENT_VERSION,
                        Long.toString(System.currentTimeMillis()),
                        nodeId,
                        clusterNodeAddress.getHost(),
                        Integer.toString(clusterNodeAddress.getPort()));

        final ClusterNodeFile clusterNodeFile = ClusterNodeFile.parseClusterNodeFile(clusterNodeFileString);
        Assert.assertNull(clusterNodeFile);
    }

    @Test(expected = NullPointerException.class)
    public void test_parseClusterNodeFile_null() {
        ClusterNodeFile.parseClusterNodeFile(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_parseClusterNodeFile_blank() {
        ClusterNodeFile.parseClusterNodeFile("  ");
    }

    public static String createClusterNodeFileString(final String version, final String timeInMillis, final String nodeId, final String host, final String port) {

        final String content =
                version + ClusterNodeFile.CONTENT_SEPARATOR
                + timeInMillis + ClusterNodeFile.CONTENT_SEPARATOR
                + nodeId + ClusterNodeFile.CONTENT_SEPARATOR
                + host + ClusterNodeFile.CONTENT_SEPARATOR
                + port + ClusterNodeFile.CONTENT_SEPARATOR;

        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    private String createClusterNodeFileStringTooLong(final String version, final String timeInMillis, final String nodeId, final String host, final String port) {

        final String content =
                version + ClusterNodeFile.CONTENT_SEPARATOR
                + timeInMillis + ClusterNodeFile.CONTENT_SEPARATOR
                + nodeId + ClusterNodeFile.CONTENT_SEPARATOR
                + host + ClusterNodeFile.CONTENT_SEPARATOR
                + port + ClusterNodeFile.CONTENT_SEPARATOR
                + port + ClusterNodeFile.CONTENT_SEPARATOR;

        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    private String createClusterNodeFileStringTooShort(final String version, final String timeInMillis, final String nodeId, final String host, final String port) {

        final String content =
                version + ClusterNodeFile.CONTENT_SEPARATOR
                + timeInMillis + ClusterNodeFile.CONTENT_SEPARATOR;

        return new String(Base64.getEncoder().encode(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }
}