package com.hivemq.plugin.aws;

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

public class S3ClientTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public PluginInformation pluginInformation;
    @Mock
    public S3Client s3Client;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(pluginInformation.getPluginHomeFolder()).thenReturn(temporaryFolder.getRoot());
    }

    @Test
    public void test_create_successful() throws IOException {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(pluginInformation);
        final S3Client s3Client = new S3Client(configurationReader);
        Assert.assertNotNull(s3Client);
    }

    @Test(expected = IllegalStateException.class)
    public void test_create_no_config_file() {
        final ConfigurationReader configurationReader = new ConfigurationReader(pluginInformation);
        new S3Client(configurationReader);
    }

    @Test(expected = IllegalStateException.class)
    public void test_create_invalid_config() throws IOException {
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-12345");
        }
        final ConfigurationReader configurationReader = new ConfigurationReader(pluginInformation);
        new S3Client(configurationReader);
    }
}