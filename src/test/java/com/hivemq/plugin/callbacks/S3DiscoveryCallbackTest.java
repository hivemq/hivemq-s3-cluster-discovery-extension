package com.hivemq.plugin.callbacks;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.hivemq.plugin.api.parameter.PluginInformation;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.plugin.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.plugin.aws.S3Client;
import com.hivemq.plugin.config.ClusterNodeFile;
import com.hivemq.plugin.config.ConfigurationReader;
import com.hivemq.plugin.config.S3Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.PrintWriter;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(S3DiscoveryCallback.class)
public class S3DiscoveryCallbackTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public PluginInformation pluginInformation;
    @Mock
    public S3Client s3Client;
    @Mock
    public ClusterDiscoveryInput clusterDiscoveryInput;
    @Mock
    public ClusterDiscoveryOutput clusterDiscoveryOutput;

    private ConfigurationReader configurationReader;
    private S3DiscoveryCallback s3DiscoveryCallback;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(pluginInformation.getPluginHomeFolder()).thenReturn(temporaryFolder.getRoot());
        configurationReader = new ConfigurationReader(pluginInformation);

        Mockito.doNothing().when(s3Client).saveObject(any(), any());
        Mockito.doNothing().when(s3Client).deleteObject(any());
        Mockito.when(s3Client.getObject(any())).thenReturn(new S3Object());
        Mockito.when(s3Client.getObjects(any())).thenReturn(new ObjectListing());
        Mockito.when(s3Client.getNextBatchOfObjects(any())).thenReturn(new ObjectListing());

        Mockito.when(clusterDiscoveryInput.getOwnClusterId()).thenReturn("ABCD12");
        Mockito.when(clusterDiscoveryInput.getOwnAddress()).thenReturn(new ClusterNodeAddress("127.0.0.1", 7800));
    }

    @Test
    public void test_init_success() throws Exception {
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        final S3Config s3Config = configurationReader.readConfiguration();
        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
        Mockito.when(s3Client.doesBucketExist()).thenReturn(true);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_config_invalid() throws Exception {
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default1234");
        }

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        final S3Config s3Config = configurationReader.readConfiguration();
        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
        Mockito.when(s3Client.doesBucketExist()).thenReturn(true);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, times(0)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_no_config() {
        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, times(0)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_init_bucket_does_not_exist() throws Exception {
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        final S3Config s3Config = configurationReader.readConfiguration();
        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
        Mockito.when(s3Client.doesBucketExist()).thenReturn(false);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput).setReloadInterval(30);
        verify(clusterDiscoveryOutput, times(0)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_same_config() throws Exception {
        test_init_success();

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        final S3Config s3Config = configurationReader.readConfiguration();
        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
        Mockito.when(s3Client.doesBucketExist()).thenReturn(true);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_new_config() throws Exception {
        test_init_success();

        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:120");
            printWriter.println("update-interval:60");
            printWriter.println("credentials-type:default");
        }

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        final S3Config s3Config = configurationReader.readConfiguration();
        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
        Mockito.when(s3Client.doesBucketExist()).thenReturn(true);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_success_new_config_no_bucket_reuse_client() throws Exception {
        test_init_success();

        deleteFilesInTemporaryFolder();

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-2");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:120");
            printWriter.println("update-interval:60");
            printWriter.println("credentials-type:default");
        }

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        final S3Config s3Config = configurationReader.readConfiguration();
        Mockito.when(s3Client.getS3Config()).thenReturn(s3Config);
        Mockito.when(s3Client.doesBucketExist()).thenReturn(true);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_no_config_reuse_client() throws Exception {
        test_init_success();

        deleteFilesInTemporaryFolder();

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    public void test_reload_no_config_no_client() throws Exception {
        test_init_no_config();

        s3Client = PowerMockito.mock(S3Client.class);
        PowerMockito.whenNew(S3Client.class).withAnyArguments().thenReturn(s3Client);

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(0)).provideCurrentNodes(anyList());
    }

    private void deleteFilesInTemporaryFolder() {
        final String root = temporaryFolder.getRoot().getAbsolutePath();
        // deletes also root folder
        temporaryFolder.delete();
        // restore root folder
        new File(root).mkdir();
    }
}