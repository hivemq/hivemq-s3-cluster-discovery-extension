package com.hivemq.plugin.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.hivemq.plugin.api.parameter.PluginInformation;
import com.hivemq.plugin.config.ConfigurationReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class S3ClientTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public PluginInformation pluginInformation;
    @Mock
    public S3Client s3Client;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(pluginInformation.getPluginHomeFolder()).thenReturn(temporaryFolder.getRoot());

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(pluginInformation);
        s3Client = new S3Client(configurationReader);
    }

    @Test
    public void test_create_successful() throws IOException {

        s3Client.createOrUpdate();
        Assert.assertNotNull(s3Client);
    }

    @Test
    public void test_bucket_exists() throws IOException {

        s3Client.createOrUpdate();
        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;


        when(amazonS3.doesBucketExistV2(anyString())).thenReturn(true);
        final boolean bucketExist = s3Client.doesBucketExist();

        assertTrue(bucketExist);

    }

    @Test
    public void test_bucket_not_exists() throws IOException {

        s3Client.createOrUpdate();
        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;


        when(amazonS3.doesBucketExistV2(anyString())).thenReturn(false);
        final boolean bucketExist = s3Client.doesBucketExist();

        assertFalse(bucketExist);

    }

    @Test(expected = IllegalStateException.class)
    public void test_create_no_config_file() {
        temporaryFolder.delete();
        final ConfigurationReader configurationReader = new ConfigurationReader(pluginInformation);
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();
    }

    @Test(expected = IllegalStateException.class)
    public void test_create_invalid_config() throws IOException {
        temporaryFolder.delete();
        temporaryFolder.create();
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-12345");
        }
        final ConfigurationReader configurationReader = new ConfigurationReader(pluginInformation);
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();
    }
}