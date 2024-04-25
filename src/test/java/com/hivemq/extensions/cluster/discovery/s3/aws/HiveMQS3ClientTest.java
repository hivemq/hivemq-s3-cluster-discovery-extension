/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.cluster.discovery.s3.aws;

import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extensions.cluster.discovery.s3.config.AuthenticationType;
import com.hivemq.extensions.cluster.discovery.s3.config.ConfigurationReader;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
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
import java.util.function.Consumer;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HiveMQS3ClientTest {

    private @NotNull ExtensionInformation extensionInformation;
    private @NotNull HiveMQS3Client hiveMQS3Client;

    @BeforeEach
    void setUp(@TempDir final @NotNull File tempDir) throws IOException {
        extensionInformation = mock(ExtensionInformation.class);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir);

        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
    }

    @Test
    void test_create_successful() {
        hiveMQS3Client.createOrUpdate();
        assertNotNull(hiveMQS3Client.getS3Config());
        assertNotNull(hiveMQS3Client.getS3Client());
    }

    @Test
    void test_bucket_exists() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        final HeadBucketResponse headBucketResponse = mock(HeadBucketResponse.class);
        final SdkHttpResponse sdkHttpResponse = mock(SdkHttpResponse.class);
        when(headBucketResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(true);
        when(sdkHttpResponse.statusCode()).thenReturn(200);
        when(s3Client.headBucket(ArgumentMatchers.<Consumer<HeadBucketRequest.Builder>>any())).thenReturn(
                headBucketResponse);
        final S3BucketResponse s3Bucket = hiveMQS3Client.checkBucket();

        assertTrue(s3Bucket.isSuccessful());
        assertEquals(S3BucketResponse.Status.EXISTING, s3Bucket.getStatus());
        assertEquals("hivemq123456", s3Bucket.getBucketName());
        assertTrue(s3Bucket.getThrowable().isEmpty());
    }

    @Test
    void test_bucket_not_exists() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        final AwsServiceException awsServiceException = S3Exception.builder()
                .message("Bucket not found!")
                .awsErrorDetails(AwsErrorDetails.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                        .build())
                .build();
        when(s3Client.headBucket(ArgumentMatchers.<Consumer<HeadBucketRequest.Builder>>any())).thenThrow(
                awsServiceException);
        final S3BucketResponse s3Bucket = hiveMQS3Client.checkBucket();

        assertFalse(s3Bucket.isSuccessful());
        assertEquals(S3BucketResponse.Status.NOT_EXISTING, s3Bucket.getStatus());
        assertEquals("hivemq123456", s3Bucket.getBucketName());
        assertTrue(s3Bucket.getThrowable().isPresent());
        assertEquals(awsServiceException, s3Bucket.getThrowable().get());
    }

    @Test
    void test_create_no_config_file() throws IOException {
        Files.delete(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION));

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    void test_create_invalid_config() throws IOException {
        final String configuration = "s3-bucket-region:us-east-12345\n";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    void createOrUpdate_identicalConfig_sameClient() throws IOException {
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);

        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        final S3Client firstClient = hiveMQS3Client.getS3Client();

        hiveMQS3Client.createOrUpdate();
        final S3Client secondClient = hiveMQS3Client.getS3Client();
        assertSame(firstClient, secondClient);
    }

    @Test
    void createOrUpdate_differentConfig_differentClient() throws IOException {
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);

        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        final S3Client firstClient = hiveMQS3Client.getS3Client();

        final String configuration2 = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq654321\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration2);
        hiveMQS3Client.createOrUpdate();
        final S3Client secondClient = hiveMQS3Client.getS3Client();
        assertNotSame(firstClient, secondClient);
    }

    @Test
    void test_getAwsCredentials_default() {
        final AwsCredentialsProvider awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.DEFAULT);
        //noinspection resource
        assertInstanceOf(DefaultCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_environment() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.ENVIRONMENT_VARIABLES);
        assertInstanceOf(EnvironmentVariableCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_java_system() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.JAVA_SYSTEM_PROPERTIES);
        assertInstanceOf(SystemPropertyCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_user_credentials() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.USER_CREDENTIALS_FILE);
        //noinspection resource
        assertInstanceOf(ProfileCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_instance_profile() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.INSTANCE_PROFILE_CREDENTIALS);
        //noinspection resource
        assertInstanceOf(InstanceProfileCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_access_key_success() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        final AwsCredentialsProvider awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY);
        assertInstanceOf(StaticCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_access_key_missing_secret() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    void test_getAwsCredentials_access_key_missing_id() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    void test_getAwsCredentials_temporary_session_success() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:temporary_session\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key\n" +
                "credentials-session-token:session-token";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.TEMPORARY_SESSION);
        assertInstanceOf(StaticCredentialsProvider.class, awsCredentials);
    }

    @Test
    void test_getAwsCredentials_temporary_session_missing_token() throws IOException {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:temporary_session\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThrows(IllegalStateException.class, () -> hiveMQS3Client.createOrUpdate());
    }

    @Test
    void test_saveObject_success() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.putObject(ArgumentMatchers.<Consumer<PutObjectRequest.Builder>>any(),
                any(RequestBody.class))).thenReturn(PutObjectResponse.builder().build());
        hiveMQS3Client.saveObject("abcd", "test");
    }

    @Test
    void test_saveObject_objectkey_null() {
        System.setProperty("aws.accessKeyId", "asdf");
        System.setProperty("aws.secretAccessKey", "asdf");
        hiveMQS3Client.createOrUpdate();
        //noinspection DataFlowIssue
        assertThrows(SdkClientException.class, () -> hiveMQS3Client.saveObject(null, "test"));
    }

    @Test
    void test_saveObject_content_null() {
        System.setProperty("aws.accessKeyId", "asdf");
        System.setProperty("aws.secretAccessKey", "asdf");
        hiveMQS3Client.createOrUpdate();
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> hiveMQS3Client.saveObject("abcd", null));
    }

    @Test
    void test_getObject_success() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.getObjectAsBytes(ArgumentMatchers.<Consumer<GetObjectRequest.Builder>>any())).thenReturn(
                ResponseBytes.fromByteArray(mock(GetObjectResponse.class), "Test".getBytes(StandardCharsets.UTF_8)));
        final String abcd = hiveMQS3Client.getObject("abcd");
        assertNotNull(abcd);
    }

    @Test
    void test_getObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.getObjectAsBytes(ArgumentMatchers.<Consumer<GetObjectRequest.Builder>>any())).thenThrow(
                IllegalArgumentException.class);
        //noinspection DataFlowIssue
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.getObject(null));
    }

    @Test
    void test_deleteObject_success() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.deleteObject(ArgumentMatchers.<Consumer<DeleteObjectRequest.Builder>>any())).thenReturn(mock(
                DeleteObjectResponse.class));
        hiveMQS3Client.deleteObject("abcd");
    }

    @Test
    void test_deleteObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        doThrow(IllegalArgumentException.class).when(s3Client)
                .deleteObject(ArgumentMatchers.<Consumer<DeleteObjectRequest.Builder>>any());
        //noinspection DataFlowIssue
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.deleteObject(null));
    }

    @Test
    void test_listObjects_success() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenReturn(mock(
                ListObjectsV2Response.class));
        final ListObjectsV2Response abcd = hiveMQS3Client.getObjects();
        assertNotNull(abcd);
    }

    @Test
    void test_listObjects_fileprefix_null() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenThrow(
                IllegalArgumentException.class);
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.getObjects());
    }

    @Test
    void test_listNextBatchOfObjects_success() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenReturn(mock(
                ListObjectsV2Response.class));
        final ListObjectsV2Response nextBatchOfObjects = hiveMQS3Client.getNextBatchOfObjects("continuationToken");
        assertNotNull(nextBatchOfObjects);
    }

    @Test
    void test_listNextBatchOfObjects_objectlisting_null() {
        hiveMQS3Client.createOrUpdate();
        final S3Client s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenThrow(
                IllegalArgumentException.class);
        //noinspection DataFlowIssue
        assertThrows(IllegalArgumentException.class, () -> hiveMQS3Client.getNextBatchOfObjects(null));
    }
}
