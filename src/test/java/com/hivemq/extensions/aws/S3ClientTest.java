package com.hivemq.extensions.aws;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extensions.config.AuthenticationType;
import com.hivemq.extensions.config.ConfigurationReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ClientTest {

    private @NotNull ExtensionInformation extensionInformation;
    private @NotNull HiveMQS3Client hiveMQS3Client;

    @BeforeEach
    public void setUp(@TempDir final @NotNull File tempDir) throws IOException {
        extensionInformation = mock(ExtensionInformation.class);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir);

        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
    }

    @Test
    public void test_create_successful() {
        hiveMQS3Client.createOrUpdate();
        assertNotNull(hiveMQS3Client.getS3Config());
        assertNotNull(hiveMQS3Client.s3Client);
    }

    @Test
    public void test_bucket_exists() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        final HeadBucketResponse headBucketResponse = mock(HeadBucketResponse.class);
        final SdkHttpResponse sdkHttpResponse = mock(SdkHttpResponse.class);
        when(headBucketResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(true);
        when(Objects.requireNonNull(hiveMQS3Client.s3Client).headBucket(any(HeadBucketRequest.class))).thenReturn(
                headBucketResponse);
        final boolean bucketExist = hiveMQS3Client.existsBucket();

        assertTrue(bucketExist);
    }

    @Test
    public void test_bucket_not_exists() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(Objects.requireNonNull(hiveMQS3Client.s3Client).headBucket(any(HeadBucketRequest.class))).thenThrow(
                S3Exception.builder().message("Bucket not found!").build());
        final boolean bucketExist = hiveMQS3Client.existsBucket();

        assertFalse(bucketExist);
    }

    @Test
    public void test_create_no_config_file() {
        assertTrue(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE)
                .toFile()
                .delete());

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    public void test_create_invalid_config() throws IOException {
        final String configuration = "s3-bucket-region:us-east-12345\n";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    public void test_getAwsCredentials_default() {
        final AwsCredentialsProvider awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.DEFAULT);
        assertTrue(awsCredentials instanceof DefaultCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_environment() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.ENVIRONMENT_VARIABLES);
        assertTrue(awsCredentials instanceof EnvironmentVariableCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_java_system() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.JAVA_SYSTEM_PROPERTIES);
        assertTrue(awsCredentials instanceof SystemPropertyCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_user_credentials() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.USER_CREDENTIALS_FILE);
        assertTrue(awsCredentials instanceof ProfileCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_instance_profile() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.INSTANCE_PROFILE_CREDENTIALS);
        assertTrue(awsCredentials instanceof InstanceProfileCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_access_key_success() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        final AwsCredentialsProvider awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY);
        assertTrue(awsCredentials instanceof StaticCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_access_key_missing_secret() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        assertThrows(IllegalStateException.class,
                () -> hiveMQS3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY));
    }

    @Test
    public void test_getAwsCredentials_access_key_missing_id() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        assertThrows(IllegalStateException.class,
                () -> hiveMQS3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY));
    }

    @Test
    public void test_getAwsCredentials_temporary_session_success() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:temporary_session\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key\n" +
                "credentials-session-token:session-token";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.TEMPORARY_SESSION);
        assertTrue(awsCredentials instanceof StaticCredentialsProvider);
    }

    @Test
    public void test_getAwsCredentials_temporary_session_missing_token() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:temporary_session\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder()
                .toPath()
                .resolve(ConfigurationReader.S3_CONFIG_FILE), configuration, StandardOpenOption.CREATE_NEW);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        assertThrows(IllegalStateException.class,
                () -> hiveMQS3Client.getAwsCredentials(AuthenticationType.TEMPORARY_SESSION));
    }

    @Test
    public void test_saveObject_success() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(
                PutObjectResponse.builder().build());
        hiveMQS3Client.saveObject("abcd", "test");
    }

    @Test
    public void test_saveObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.saveObject(null, "test"));
    }

    @Test
    public void test_saveObject_content_null() {
        hiveMQS3Client.createOrUpdate();
        assertThrows(NullPointerException.class, () -> hiveMQS3Client.saveObject("abcd", null));
    }

    @Test
    public void test_getObject_success() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.getObjectAsBytes(any(GetObjectRequest.class))).thenReturn(ResponseBytes.fromByteArray(
                mock(GetObjectResponse.class),
                "Test".getBytes(StandardCharsets.UTF_8)));
        final String abcd = hiveMQS3Client.getObject("abcd");
        assertNotNull(abcd);
    }

    @Test
    public void test_getObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.getObject(any(GetObjectRequest.class))).thenThrow(IllegalArgumentException.class);
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.getObject(null));
    }

    @Test
    public void test_deleteObject_success() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        doNothing().when(hiveMQS3Client.s3Client).deleteObject(any(DeleteObjectRequest.class));
        hiveMQS3Client.deleteObject("abcd");
    }

    @Test
    public void test_deleteObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        doThrow(IllegalArgumentException.class).when(hiveMQS3Client.s3Client)
                .deleteObject(any(DeleteObjectRequest.class));
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.deleteObject(null));
    }

    @Test
    public void test_listObjects_success() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mock(
                ListObjectsV2Response.class));
        final ListObjectsV2Response abcd = hiveMQS3Client.getObjects();
        assertNotNull(abcd);
    }

    @Test
    public void test_listObjects_fileprefix_null() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(IllegalArgumentException.class);
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.getObjects());
    }

    @Test
    public void test_listNextBatchOfObjects_success() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mock(
                ListObjectsV2Response.class));
        final ListObjectsV2Response nextBatchOfObjects = hiveMQS3Client.getNextBatchOfObjects("continuationToken");
        assertNotNull(nextBatchOfObjects);
    }

    @Test
    public void test_listNextBatchOfObjects_objectlisting_null() {
        hiveMQS3Client.createOrUpdate();
        hiveMQS3Client.s3Client = mock(S3Client.class);

        when(hiveMQS3Client.s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(IllegalArgumentException.class);
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.getNextBatchOfObjects(null));
    }
}
