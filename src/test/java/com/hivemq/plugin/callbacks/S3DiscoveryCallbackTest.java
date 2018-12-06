package com.hivemq.plugin.callbacks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.hivemq.plugin.api.parameter.PluginInformation;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.plugin.aws.S3Client;
import com.hivemq.plugin.config.ConfigurationReader;
import com.hivemq.plugin.config.S3Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.PrintWriter;

import static org.mockito.Mockito.*;

public class S3DiscoveryCallbackTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public PluginInformation pluginInformation;
    @Mock
    public S3Client s3Client;
    @Mock
    AmazonS3 amazonS3;
    @Mock
    public ClusterDiscoveryInput clusterDiscoveryInput;
    @Mock
    public ClusterDiscoveryOutput clusterDiscoveryOutput;

    private S3DiscoveryCallback s3DiscoveryCallback;
    private ConfigurationReader configurationReader;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(clusterDiscoveryInput.getOwnClusterId()).thenReturn("ABCD12");
        when(clusterDiscoveryInput.getOwnAddress()).thenReturn(new ClusterNodeAddress("127.0.0.1", 7800));

        when(pluginInformation.getPluginHomeFolder()).thenReturn(temporaryFolder.getRoot());

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        configurationReader = new ConfigurationReader(pluginInformation);
        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3Client.configurationReader = configurationReader;
        s3DiscoveryCallback.s3Client = s3Client;

        s3Client.configurationReader.readConfiguration();
        s3Client.amazonS3 = amazonS3;

//        final S3Config s3Config = configurationReader.readConfiguration();
//        when(s3Client.getS3Config()).thenReturn(s3Config);
        when(s3Client.doesBucketExist()).thenReturn(true);
    }

    @Test
    public void test_init_success() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_bucket_does_not_exist() {

        when(s3Client.doesBucketExist()).thenReturn(false);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_create_failed_config() {

        doThrow(new IllegalStateException("Config is not valid.")).when(s3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, never()).doesBucketExist();
        verify(s3Client).createOrUpdate();

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_create_failed_by_amazons3() {

        doThrow(new AmazonS3Exception("AmazonS3 couldn't be build.")).when(s3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, never()).doesBucketExist();
        verify(s3Client).createOrUpdate();

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_bucket_check_failed() {

        when(s3Client.doesBucketExist()).thenReturn(false);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_config_invalid() throws Exception {

        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq1234");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, never()).doesBucketExist();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_no_config() {
        temporaryFolder.delete();

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_same_config() throws Exception {

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

//        final S3Config s3Config = configurationReader.readConfiguration();
//        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
//        Mockito.when(s3Client.doesBucketExist()).thenReturn(true);

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_new_config() throws Exception {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:120");
            printWriter.println("update-interval:60");
            printWriter.println("credentials-type:default");
        }

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_new_config_no_bucket_reuse_client() throws Exception {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:120");
            printWriter.println("update-interval:60");
            printWriter.println("credentials-type:default");
        }

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_new_config_no_bucket_no_existing_client() throws Exception {

        temporaryFolder.delete();
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:120");
            printWriter.println("update-interval:60");
            printWriter.println("credentials-type:default");
        }

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(1)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_no_config_reuse_client() throws Exception {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        deleteFilesInTemporaryFolder();

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_no_config_no_existing_client() {
        test_init_no_config();

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    private void deleteFilesInTemporaryFolder() {
        final String root = temporaryFolder.getRoot().getAbsolutePath();
        // deletes also root folder
        temporaryFolder.delete();
        // restore root folder
        new File(root).mkdir();
    }
}