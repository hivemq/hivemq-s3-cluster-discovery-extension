package com.hivemq.extensions.aws;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extensions.config.AuthenticationType;
import com.hivemq.extensions.config.ConfigurationReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class S3ClientTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public ExtensionInformation extensionInformation;

    private S3Client s3Client;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(temporaryFolder.getRoot());

        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
        }

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        s3Client = new S3Client(configurationReader);
    }

    @Test
    public void test_create_successful() {
        s3Client.createOrUpdate();
        assertNotNull(s3Client.getS3Config());
        assertNotNull(s3Client.amazonS3);
    }

    @Test
    public void test_bucket_exists() {
        s3Client.createOrUpdate();
        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.doesBucketExistV2(anyString())).thenReturn(true);
        final boolean bucketExist = s3Client.existsBucket();

        assertTrue(bucketExist);
    }

    @Test
    public void test_bucket_not_exists() {
        s3Client.createOrUpdate();
        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.doesBucketExistV2(anyString())).thenThrow(new AmazonS3Exception("Bucket not found!"));
        final boolean bucketExist = s3Client.existsBucket();

        assertFalse(bucketExist);
    }

    @Test(expected = IllegalStateException.class)
    public void test_create_no_config_file() {
        temporaryFolder.delete();
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
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
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();
    }

    @Test
    public void test_getAwsCredentials_default() {
        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.DEFAULT);
        assertTrue(awsCredentials instanceof DefaultAWSCredentialsProviderChain);
    }

    @Test
    public void test_getAwsCredentials_environment() {
        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.ENVIRONMENT_VARIABLES);
        assertTrue(awsCredentials instanceof EnvironmentVariableCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_java_system() {
        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.JAVA_SYSTEM_PROPERTIES);
        assertTrue(awsCredentials instanceof SystemPropertiesCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_user_credentials() {
        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.USER_CREDENTIALS_FILE);
        assertTrue(awsCredentials instanceof ProfileCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_instance_profile() {
        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.INSTANCE_PROFILE_CREDENTIALS);
        assertTrue(awsCredentials instanceof InstanceProfileCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_access_key_success() throws IOException {
        deleteFilesInTemporaryFolder();
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
            printWriter.println("credentials-type:access_key");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:secret-access-key");
        }
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();

        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY);
        assertTrue(awsCredentials instanceof AWSStaticCredentialsProvider);
    }

    @Test(expected = IllegalStateException.class)
    public void test_getAwsCredentials_access_key_missing_secret() throws IOException {
        deleteFilesInTemporaryFolder();
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
            printWriter.println("credentials-type:access_key");
            printWriter.println("credentials-access-key-id:access-key-id");
        }
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();
        s3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY);
    }

    @Test(expected = IllegalStateException.class)
    public void test_getAwsCredentials_access_key_missing_id() throws IOException {
        deleteFilesInTemporaryFolder();
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
            printWriter.println("credentials-type:access_key");
            printWriter.println("credentials-secret-access-key:secret-access-key");
        }
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();
        s3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY);
    }

    @Test
    public void test_getAwsCredentials_temporary_session_success() throws IOException {
        deleteFilesInTemporaryFolder();
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
            printWriter.println("credentials-type:temporary_session");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:secret-access-key");
            printWriter.println("credentials-session-token:session-token");
        }
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();

        final AWSCredentialsProvider awsCredentials = s3Client.getAwsCredentials(AuthenticationType.TEMPORARY_SESSION);
        assertTrue(awsCredentials instanceof AWSStaticCredentialsProvider);
    }

    @Test(expected = IllegalStateException.class)
    public void test_getAwsCredentials_temporary_session_missing_token() throws IOException {
        deleteFilesInTemporaryFolder();
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        try (final PrintWriter printWriter = new PrintWriter(temporaryFolder.newFile(ConfigurationReader.S3_CONFIG_FILE))) {
            printWriter.println("s3-bucket-region:us-east-1");
            printWriter.println("s3-bucket-name:hivemq123456");
            printWriter.println("file-prefix:hivemq/cluster/nodes/");
            printWriter.println("file-expiration:360");
            printWriter.println("update-interval:180");
            printWriter.println("credentials-type:default");
            printWriter.println("credentials-type:temporary_session");
            printWriter.println("credentials-access-key-id:access-key-id");
            printWriter.println("credentials-secret-access-key:secret-access-key");
        }
        s3Client = new S3Client(configurationReader);
        s3Client.createOrUpdate();
        s3Client.getAwsCredentials(AuthenticationType.TEMPORARY_SESSION);
    }

    private void deleteFilesInTemporaryFolder() {
        final String root = temporaryFolder.getRoot().getAbsolutePath();
        // deletes also root folder
        temporaryFolder.delete();
        // restore root folder
        new File(root).mkdir();
    }

    @Test
    public void test_saveObject_success() throws Exception {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.putObject(any(), any(), any(), any())).thenReturn(new PutObjectResult());
        s3Client.saveObject("abcd", "test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_saveObject_objectkey_null() throws Exception {
        s3Client.createOrUpdate();
        s3Client.saveObject(null, "test");
    }

    @Test(expected = NullPointerException.class)
    public void test_saveObject_content_null() throws Exception {
        s3Client.createOrUpdate();
        s3Client.saveObject("abcd", null);
    }

    @Test
    public void test_getObject_success() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.getObject(anyString(), any())).thenReturn(new S3Object());
        final S3Object abcd = s3Client.getObject("abcd");
        assertNotNull(abcd);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getObject_objectkey_null() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.getObject(anyString(), any())).thenThrow(IllegalArgumentException.class);
        s3Client.getObject(null);
    }

    @Test
    public void test_deleteObject_success() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        doNothing().when(amazonS3).deleteObject(anyString(), any());
        s3Client.deleteObject("abcd");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_deleteObject_objectkey_null() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        doThrow(IllegalArgumentException.class).when(amazonS3).deleteObject(anyString(), any());
        s3Client.deleteObject(null);
    }

    @Test
    public void test_listObjects_success() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.listObjects(any(), any())).thenReturn(new ObjectListing());
        final ObjectListing abcd = s3Client.getObjects("abcd");
        assertNotNull(abcd);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_listObjects_fileprefix_null() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.listObjects(any(), any())).thenThrow(IllegalArgumentException.class);
        s3Client.getObjects(null);
    }

    @Test
    public void test_listNextBatchOfObjects_success() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.listNextBatchOfObjects(any(ObjectListing.class))).thenReturn(new ObjectListing());
        final ObjectListing nextBatchOfObjects = s3Client.getNextBatchOfObjects(new ObjectListing());
        assertNotNull(nextBatchOfObjects);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_listNextBatchOfObjects_objectlisting_null() {
        s3Client.createOrUpdate();

        final AmazonS3 amazonS3 = Mockito.mock(AmazonS3.class);
        s3Client.amazonS3 = amazonS3;

        when(amazonS3.listNextBatchOfObjects(any(ObjectListing.class))).thenThrow(IllegalArgumentException.class);
        s3Client.getNextBatchOfObjects(null);
    }
}