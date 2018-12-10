package com.hivemq.extension.config;

import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.PrintWriter;

public class ConfigurationReaderTest {

    @Mock
    public ExtensionInformation extensionInformation;
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(extensionInformation.getExtensionHomeFolder()).thenReturn(temporaryFolder.getRoot());
    }

    @Test
    public void test_readConfiguration_no_file() {
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_successful() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_missing_bucket_name() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_missing_bucket_region() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_invalid_bucket_region() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-123456");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_missing_credentials_type() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_invalid_credentials_type() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default1234");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_credentials_type_access_key_successful() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:access_key");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:secret-access-key");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_credentials_type_access_key_missing_key() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:access_key");
            printWriter.println("credentials-access-key-id:");
            printWriter.println("credentials-secret-access-key:secret-access-key");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_credentials_type_access_key_missing_secret() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:access_key");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_credentials_type_temporary_session_successful() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:temporary_session");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:secret-access-key");
            printWriter.println("credentials-session-token:session-token");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_credentials_type_temporary_session_missing_session_token() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:temporary_session");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:secret-access-key");
            printWriter.println("credentials-session-token:");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_both_intervals_zero_successful() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:0");
            printWriter.println("update-interval:0");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_both_intervals_same_value() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:180");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_update_interval_larger() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:150");
            printWriter.println("update-interval:300");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_update_deactivated() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:180");
            printWriter.println("update-interval:0");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_expiration_deactivated() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:0");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_missing_expiration() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }

    @Test
    public void test_readConfiguration_missing_update() throws Exception {

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:180");
            printWriter.println("update-interval:");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        Assert.assertNull(configurationReader.readConfiguration());
    }
}