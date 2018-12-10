package com.hivemq.extension.callbacks;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.extension.aws.S3Client;
import com.hivemq.extension.config.ClusterNodeFile;
import com.hivemq.extension.config.ClusterNodeFileTest;
import com.hivemq.extension.config.ConfigurationReader;
import com.hivemq.extension.config.S3Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class S3DiscoveryCallbackTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public ExtensionInformation extensionInformation;
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
    private S3Config s3Config;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(clusterDiscoveryInput.getOwnClusterId()).thenReturn("ABCD12");
        when(clusterDiscoveryInput.getOwnAddress()).thenReturn(new ClusterNodeAddress("127.0.0.1", 7800));

        when(extensionInformation.getExtensionHomeFolder()).thenReturn(temporaryFolder.getRoot());

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        configurationReader = new ConfigurationReader(extensionInformation);
        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3Client.configurationReader = configurationReader;
        s3DiscoveryCallback.s3Client = s3Client;

        s3Client.configurationReader.readConfiguration();
        s3Client.amazonS3 = amazonS3;

        s3Config = configurationReader.readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);
        when(s3Client.doesBucketExist()).thenReturn(true);
    }

    @Test
    public void test_init_success() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        when(s3Client.getObject(any())).thenReturn(createS3Object());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_provide_current_nodes_exception_getting_node_files() {
        doThrow(AmazonS3Exception.class).when(s3Client).getObjects(any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_amazons3exception_getting_node_file() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        doThrow(AmazonS3Exception.class).when(s3Client).getObject(any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_exception_getting_node_file() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        doThrow(Exception.class).when(s3Client).getObject(any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_s3objectsummary_null() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtendedNullObjects());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_expired_files() throws Exception {
        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:2");
            printWriter.println("update-interval:1");
            printWriter.println("credentials-type:default");
        }
        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);

        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        when(s3Client.getObject(any())).thenReturn(createS3Object());

        // Wait for files to expire
        TimeUnit.SECONDS.sleep(2);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_truncated() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingTruncated());
        when(s3Client.getObject(any())).thenReturn(createS3Object());
        when(s3Client.getNextBatchOfObjects(any())).thenReturn(new ObjectListingNotTruncated());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_provide_current_nodes_s3object_null() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        when(s3Client.getObject(any())).thenReturn(null);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_s3object_content_blank() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        when(s3Client.getObject(any())).thenReturn(createS3ObjectBlankContent());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_s3object_content_null() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        when(s3Client.getObject(any())).thenReturn(createS3ObjectNullContent());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_provide_current_nodes_parse_failed() {
        when(s3Client.getObjects(any())).thenReturn(new ObjectListingExtended());
        when(s3Client.getObject(any())).thenReturn(createS3ObjectInvalid());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    public void test_init_save_own_file_failed() throws Exception {
        doThrow(AmazonS3Exception.class).when(s3Client).saveObject(any(), any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_bucket_does_not_exist() {

        when(s3Client.doesBucketExist()).thenReturn(false);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_create_failed_config() {

        doThrow(new IllegalStateException("Config is not valid.")).when(s3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, never()).doesBucketExist();
        verify(s3Client).createOrUpdate();

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_create_failed_by_amazons3() {

        doThrow(new AmazonS3Exception("AmazonS3 couldn't be build.")).when(s3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, never()).doesBucketExist();
        verify(s3Client).createOrUpdate();

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_bucket_check_failed() {

        when(s3Client.doesBucketExist()).thenReturn(false);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client).createOrUpdate();
        verify(s3Client).doesBucketExist();

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
        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);
        doThrow(IllegalStateException.class).when(s3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, never()).doesBucketExist();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_no_config() {
        temporaryFolder.delete();

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_same_config() throws Exception {

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
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
        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);

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
        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);
        when(s3Client.doesBucketExist()).thenReturn(false);

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(1)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_config_missing_init_success() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        deleteFilesInTemporaryFolder();

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_config_still_missing() {
        temporaryFolder.delete();
        when(s3Client.getS3Config()).thenReturn(null);
        doThrow(IllegalStateException.class).when(s3Client).createOrUpdate();

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_file_expired() throws Exception {
        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:5");
            printWriter.println("update-interval:1");
            printWriter.println("credentials-type:default");
        }
        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        // Wait for file to expire
        TimeUnit.SECONDS.sleep(1);

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, times(2)).saveObject(any(), any());
        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_file_exception() throws Exception {
        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:5");
            printWriter.println("update-interval:1");
            printWriter.println("credentials-type:default");
        }
        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(s3Client.getS3Config()).thenReturn(s3Config);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        // Wait for file to expire
        TimeUnit.SECONDS.sleep(1);
        doThrow(AmazonS3Exception.class).when(s3Client).saveObject(any(), any());

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(s3Client, times(2)).saveObject(any(), any());
        verify(clusterDiscoveryOutput, times(1)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_destroy_success() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.destroy(clusterDiscoveryInput);

        verify(s3Client, times(1)).deleteObject(any());
    }

    @Test
    public void test_destroy_no_own_file() {
        s3DiscoveryCallback.destroy(clusterDiscoveryInput);
        verify(s3Client, never()).deleteObject(any());
    }

    @Test
    public void test_destroy_delete_own_file_failed() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        when(s3Client.getObject(any())).thenReturn(new S3Object());

        doThrow(AmazonS3Exception.class).when(s3Client).deleteObject(any());
        s3DiscoveryCallback.destroy(clusterDiscoveryInput);

        verify(s3Client, times(1)).deleteObject(any());
    }

    private void deleteFilesInTemporaryFolder() {
        final String root = temporaryFolder.getRoot().getAbsolutePath();
        // deletes also root folder
        temporaryFolder.delete();
        // restore root folder
        new File(root).mkdir();
    }

    private S3Object createS3Object() {
        final ClusterNodeFile clusterNodeFile = new ClusterNodeFile("ABCD12", new ClusterNodeAddress("127.0.0.1", 1883));
        final InputStream inputStream = new ByteArrayInputStream(clusterNodeFile.toString().getBytes());

        final S3Object s3Object = new S3Object();
        s3Object.setObjectContent(inputStream);
        return s3Object;
    }

    private S3Object createS3ObjectInvalid() {
        final String clusterNodeFileString = ClusterNodeFileTest.createClusterNodeFileString("3", "3", "3", "3", "3");
        final InputStream inputStream = new ByteArrayInputStream(clusterNodeFileString.getBytes());

        final S3Object s3Object = new S3Object();
        s3Object.setObjectContent(inputStream);
        return s3Object;
    }

    private S3Object createS3ObjectBlankContent() {
        final InputStream inputStream = new ByteArrayInputStream("  ".getBytes());

        final S3Object s3Object = new S3Object();
        s3Object.setObjectContent(inputStream);
        return s3Object;
    }

    private S3Object createS3ObjectNullContent() {
        return null;
    }

    private S3ObjectSummary createS3ObjectSummary() {
        final S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey("ABCD12");
        return s3ObjectSummary;
    }

    private List<S3ObjectSummary> createObjectListing() {
        final List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        s3ObjectSummaries.add(createS3ObjectSummary());
        return s3ObjectSummaries;
    }

    class ObjectListingExtended extends ObjectListing {
        @Override
        public List<S3ObjectSummary> getObjectSummaries() {
            return createObjectListing();
        }
    }

    class ObjectListingExtendedNullObjects extends ObjectListing {
        @Override
        public List<S3ObjectSummary> getObjectSummaries() {
            final List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
            s3ObjectSummaries.add(null);
            s3ObjectSummaries.add(null);
            return s3ObjectSummaries;
        }
    }

    class ObjectListingTruncated extends ObjectListing {
        @Override
        public boolean isTruncated() {
            return true;
        }
        @Override
        public List<S3ObjectSummary> getObjectSummaries() {
            return createObjectListing();
        }
    }

    class ObjectListingNotTruncated extends ObjectListing {
        @Override
        public boolean isTruncated() {
            return false;
        }
    }
}