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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HiveMQS3ClientTest {

    private @NotNull ExtensionInformation extensionInformation;
    private @NotNull HiveMQS3Client hiveMQS3Client;

    @TempDir
    private @NotNull Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        extensionInformation = mock(ExtensionInformation.class);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir.toFile());

        final var configPath = extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION);
        Files.createDirectories(configPath.getParent());
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default""";
        Files.writeString(configPath, configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
    }

    @Test
    void test_create_successful() {
        hiveMQS3Client.createOrUpdate();
        assertThat(hiveMQS3Client.getS3Config()).isNotNull();
        assertThat(hiveMQS3Client.getS3Client()).isNotNull();
    }

    @Test
    void test_bucket_exists() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        final var headBucketResponse = mock(HeadBucketResponse.class);
        final var sdkHttpResponse = mock(SdkHttpResponse.class);
        when(headBucketResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(sdkHttpResponse.isSuccessful()).thenReturn(true);
        when(sdkHttpResponse.statusCode()).thenReturn(200);
        when(s3Client.headBucket(ArgumentMatchers.<Consumer<HeadBucketRequest.Builder>>any())).thenReturn(
                headBucketResponse);
        final var s3Bucket = hiveMQS3Client.checkBucket();

        assertThat(s3Bucket.isSuccessful()).isTrue();
        assertThat(s3Bucket.getStatus()).isEqualTo(S3BucketResponse.Status.EXISTING);
        assertThat(s3Bucket.getBucketName()).contains("hivemq123456");
        assertThat(s3Bucket.getThrowable()).isEmpty();
    }

    @Test
    void test_bucket_not_exists() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        final var awsServiceException = S3Exception.builder()
                .message("Bucket not found!")
                .awsErrorDetails(AwsErrorDetails.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                        .build())
                .build();
        when(s3Client.headBucket(ArgumentMatchers.<Consumer<HeadBucketRequest.Builder>>any())).thenThrow(
                awsServiceException);
        final var s3Bucket = hiveMQS3Client.checkBucket();

        assertThat(s3Bucket.isSuccessful()).isFalse();
        assertThat(s3Bucket.getStatus()).isEqualTo(S3BucketResponse.Status.NOT_EXISTING);
        assertThat(s3Bucket.getBucketName()).contains("hivemq123456");
        assertThat(s3Bucket.getThrowable()).hasValue(awsServiceException);
    }

    @Test
    void test_create_no_config_file() throws IOException {
        Files.delete(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION));

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThatThrownBy(() -> hiveMQS3Client.createOrUpdate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void test_create_invalid_config() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-12345
                """;
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThatThrownBy(() -> hiveMQS3Client.createOrUpdate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void createOrUpdate_identicalConfig_sameClient() throws IOException {
        final var configurationReader = new ConfigurationReader(extensionInformation);

        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        final var firstClient = hiveMQS3Client.getS3Client();

        hiveMQS3Client.createOrUpdate();
        final var secondClient = hiveMQS3Client.getS3Client();
        assertThat(secondClient).isSameAs(firstClient);
    }

    @Test
    void createOrUpdate_differentConfig_differentClient() throws IOException {
        final var configurationReader = new ConfigurationReader(extensionInformation);

        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();
        final var firstClient = hiveMQS3Client.getS3Client();

        final var configuration2 = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq654321
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration2);
        hiveMQS3Client.createOrUpdate();
        final var secondClient = hiveMQS3Client.getS3Client();
        assertThat(firstClient).isNotSameAs(secondClient);
    }

    @Test
    void test_getAwsCredentials_default() {
        final var awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.DEFAULT);
        assertThat(awsCredentials).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_environment() {
        final var awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.ENVIRONMENT_VARIABLES);
        assertThat(awsCredentials).isInstanceOf(EnvironmentVariableCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_java_system() {
        final var awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.JAVA_SYSTEM_PROPERTIES);
        assertThat(awsCredentials).isInstanceOf(SystemPropertyCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_user_credentials() {
        final var awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.USER_CREDENTIALS_FILE);
        assertThat(awsCredentials).isInstanceOf(ProfileCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_instance_profile() {
        final AwsCredentialsProvider awsCredentials =
                hiveMQS3Client.getAwsCredentials(AuthenticationType.INSTANCE_PROFILE_CREDENTIALS);
        assertThat(awsCredentials).isInstanceOf(InstanceProfileCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_access_key_success() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        final var awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.ACCESS_KEY);
        assertThat(awsCredentials).isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_access_key_missing_secret() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThatThrownBy(() -> hiveMQS3Client.createOrUpdate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void test_getAwsCredentials_access_key_missing_id() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThatThrownBy(() -> hiveMQS3Client.createOrUpdate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void test_getAwsCredentials_temporary_session_success() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:temporary_session
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key
                credentials-session-token:session-token""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        final var awsCredentials = hiveMQS3Client.getAwsCredentials(AuthenticationType.TEMPORARY_SESSION);
        assertThat(awsCredentials).isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void test_getAwsCredentials_temporary_session_missing_token() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:temporary_session
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        assertThatThrownBy(() -> hiveMQS3Client.createOrUpdate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void test_saveObject_success() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
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
        assertThatThrownBy(() -> hiveMQS3Client.saveObject(null, "test")).isInstanceOf(SdkClientException.class);
    }

    @Test
    void test_saveObject_content_null() {
        System.setProperty("aws.accessKeyId", "asdf");
        System.setProperty("aws.secretAccessKey", "asdf");
        hiveMQS3Client.createOrUpdate();
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> hiveMQS3Client.saveObject("abcd", null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void test_getObject_success() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.getObjectAsBytes(ArgumentMatchers.<Consumer<GetObjectRequest.Builder>>any())).thenReturn(
                ResponseBytes.fromByteArray(mock(GetObjectResponse.class), "Test".getBytes(StandardCharsets.UTF_8)));
        assertThat(hiveMQS3Client.getObject("abcd")).isNotNull();
    }

    @Test
    void test_getObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.getObjectAsBytes(ArgumentMatchers.<Consumer<GetObjectRequest.Builder>>any())).thenThrow(
                IllegalArgumentException.class);
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> hiveMQS3Client.getObject(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_deleteObject_success() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.deleteObject(ArgumentMatchers.<Consumer<DeleteObjectRequest.Builder>>any())).thenReturn(mock(
                DeleteObjectResponse.class));
        hiveMQS3Client.deleteObject("abcd");
    }

    @Test
    void test_deleteObject_objectkey_null() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        doThrow(IllegalArgumentException.class).when(s3Client)
                .deleteObject(ArgumentMatchers.<Consumer<DeleteObjectRequest.Builder>>any());
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> hiveMQS3Client.deleteObject(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_listObjects_success() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenReturn(mock(
                ListObjectsV2Response.class));
        assertThat(hiveMQS3Client.getObjects()).isNotNull();
    }

    @Test
    void test_listObjects_fileprefix_null() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenThrow(
                IllegalArgumentException.class);
        assertThatThrownBy(() -> hiveMQS3Client.getObjects()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_listNextBatchOfObjects_success() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenReturn(mock(
                ListObjectsV2Response.class));
        assertThat(hiveMQS3Client.getNextBatchOfObjects("continuationToken")).isNotNull();
    }

    @Test
    void test_listNextBatchOfObjects_objectlisting_null() {
        hiveMQS3Client.createOrUpdate();
        final var s3Client = mock(S3Client.class);
        hiveMQS3Client.setS3Client(s3Client);

        when(s3Client.listObjectsV2(ArgumentMatchers.<Consumer<ListObjectsV2Request.Builder>>any())).thenThrow(
                IllegalArgumentException.class);
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> hiveMQS3Client.getNextBatchOfObjects(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_createOrUpdate_defaultS3Endpoint() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default
                s3-endpoint:""" + HiveMQS3Client.S3_HOSTNAME;
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        assertThat(hiveMQS3Client.getS3Config()).isNotNull();
        assertThat(hiveMQS3Client.getS3Config().getEndpoint()).isEqualTo(HiveMQS3Client.S3_HOSTNAME);
        assertThat(hiveMQS3Client.getS3Client()).isNotNull();
    }

    @Test
    void test_createOrUpdate_customEndpointWithoutProtocol() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default
                s3-endpoint:minio.example.com:9000
                s3-endpoint-region:us-east-1""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        assertThat(hiveMQS3Client.getS3Config()).isNotNull();
        assertThat(hiveMQS3Client.getS3Config().getEndpoint()).isEqualTo("minio.example.com:9000");
        assertThat(hiveMQS3Client.getS3Config().getEndpointRegionName()).isEqualTo("us-east-1");
        assertThat(hiveMQS3Client.getS3Client()).isNotNull();
    }

    @Test
    void test_createOrUpdate_customEndpointWithHttps() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default
                s3-endpoint:https://minio.example.com:9000
                s3-endpoint-region:eu-west-1""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        assertThat(hiveMQS3Client.getS3Config()).isNotNull();
        assertThat(hiveMQS3Client.getS3Config().getEndpoint()).isEqualTo("https://minio.example.com:9000");
        assertThat(hiveMQS3Client.getS3Config().getEndpointRegionName()).isEqualTo("eu-west-1");
        assertThat(hiveMQS3Client.getS3Client()).isNotNull();
    }

    @Test
    void test_createOrUpdate_customEndpointWithHttp() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default
                s3-endpoint:http://localhost:9000
                s3-endpoint-region:local""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        assertThat(hiveMQS3Client.getS3Config()).isNotNull();
        assertThat(hiveMQS3Client.getS3Config().getEndpoint()).isEqualTo("http://localhost:9000");
        assertThat(hiveMQS3Client.getS3Config().getEndpointRegionName()).isEqualTo("local");
        assertThat(hiveMQS3Client.getS3Client()).isNotNull();
    }

    @Test
    void test_createOrUpdate_customEndpointWithoutRegion() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default
                s3-endpoint:custom.s3.example.com""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);

        // AWS SDK requires a region when using custom endpoint, so this should fail
        assertThatThrownBy(() -> hiveMQS3Client.createOrUpdate()).isInstanceOf(SdkClientException.class);
    }

    @Test
    void test_createOrUpdate_customEndpointWithPathStyleAccess() throws IOException {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq123456
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default
                s3-endpoint:https://minio.example.com
                s3-endpoint-region:us-east-1
                s3-path-style-access:true""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = new HiveMQS3Client(configurationReader);
        hiveMQS3Client.createOrUpdate();

        assertThat(hiveMQS3Client.getS3Config()).isNotNull();
        assertThat(hiveMQS3Client.getS3Config().getEndpoint()).isEqualTo("https://minio.example.com");
        assertThat(hiveMQS3Client.getS3Config().getEndpointRegionName()).isEqualTo("us-east-1");
        assertThat(hiveMQS3Client.getS3Config().getPathStyleAccess()).isTrue();
        assertThat(hiveMQS3Client.getS3Client()).isNotNull();
    }
}
