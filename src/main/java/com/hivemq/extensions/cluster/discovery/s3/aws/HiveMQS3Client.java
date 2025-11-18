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

import com.hivemq.extensions.cluster.discovery.s3.config.AuthenticationType;
import com.hivemq.extensions.cluster.discovery.s3.config.ConfigurationReader;
import com.hivemq.extensions.cluster.discovery.s3.config.S3Config;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.util.Locale;
import java.util.Objects;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_NAME;

/**
 * @since 4.0.0
 */
public class HiveMQS3Client {

    @SuppressWarnings("SpellCheckingInspection")
    static final @NotNull String S3_HOSTNAME = "s3.amazonaws.com";

    private static final @NotNull Logger LOG = LoggerFactory.getLogger(HiveMQS3Client.class);

    private final @NotNull ConfigurationReader configurationReader;

    private @Nullable S3Config s3Config;
    private @Nullable S3Client s3Client;

    public HiveMQS3Client(final @NotNull ConfigurationReader configurationReader) {
        this.configurationReader = configurationReader;
    }

    public void createOrUpdate() {
        final var newS3Config = configurationReader.readConfiguration();
        if (newS3Config == null) {
            throw new IllegalStateException("Configuration of the S3 discovery extension couldn't be loaded.");
        }
        if (s3Config != null && s3Config.equals(newS3Config)) {
            return;
        }
        s3Config = newS3Config;
        LOG.trace("{}: Configuration loaded successfully.", EXTENSION_NAME);
        final var authenticationType = AuthenticationType.fromName(s3Config.getAuthenticationTypeName());
        final var credentialsProvider = getAwsCredentials(authenticationType);
        final var s3ClientBuilder = S3Client.builder();
        if (s3Config.getEndpoint().equals(S3_HOSTNAME)) {
            final var region = Region.of(s3Config.getBucketRegionName());
            s3ClientBuilder.region(region);
        } else {
            final var lowerCaseEndpoint = s3Config.getEndpoint().toLowerCase(Locale.ROOT);
            //noinspection HttpUrlsUsage
            s3ClientBuilder.endpointOverride(URI.create(!lowerCaseEndpoint.startsWith("https://") &&
                    !lowerCaseEndpoint.startsWith("http://") ?
                    "https://" + s3Config.getEndpoint() :
                    s3Config.getEndpoint()));
            if (s3Config.getEndpointRegionName() != null) {
                final var region = Region.of(s3Config.getEndpointRegionName());
                s3ClientBuilder.region(region);
            }
        }
        final var s3ConfigurationBuilder = S3Configuration.builder();
        if (s3Config.getPathStyleAccess() != null) {
            s3ConfigurationBuilder.pathStyleAccessEnabled(s3Config.getPathStyleAccess());
        }
        if (s3Client != null) {
            s3Client.close();
        }
        s3Client = s3ClientBuilder.credentialsProvider(credentialsProvider)
                .serviceConfiguration(s3ConfigurationBuilder.build())
                .build();
        LOG.trace("{}: Created AmazonS3 client successfully.", EXTENSION_NAME);
    }

    @NotNull AwsCredentialsProvider getAwsCredentials(final @NotNull AuthenticationType authenticationType) {
        switch (authenticationType) {
            case DEFAULT:
                return DefaultCredentialsProvider.builder().build();
            case ENVIRONMENT_VARIABLES:
                return EnvironmentVariableCredentialsProvider.create();
            case JAVA_SYSTEM_PROPERTIES:
                return SystemPropertyCredentialsProvider.create();
            case USER_CREDENTIALS_FILE:
                return ProfileCredentialsProvider.create();
            case INSTANCE_PROFILE_CREDENTIALS:
                return InstanceProfileCredentialsProvider.create();
            case ACCESS_KEY:
            case TEMPORARY_SESSION:
                final var accessKey = Objects.requireNonNull(s3Config).getAccessKeyId();
                final var secretAccessKey = s3Config.getAccessKeySecret();
                if (authenticationType == AuthenticationType.ACCESS_KEY) {
                    return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretAccessKey));
                } else {
                    final var sessionToken = s3Config.getSessionToken();
                    return StaticCredentialsProvider.create(AwsSessionCredentials.create(Objects.requireNonNull(
                            accessKey), Objects.requireNonNull(secretAccessKey), Objects.requireNonNull(sessionToken)));
                }
            default:
                throw new IllegalArgumentException("Unknown credentials type.");
        }
    }

    public @NotNull S3BucketResponse checkBucket() {
        final var bucketName = Objects.requireNonNull(s3Config).getBucketName();
        try {
            final var sdkHttpResponse = Objects.requireNonNull(s3Client)
                    .headBucket(builder -> builder.bucket(bucketName).build())
                    .sdkHttpResponse();
            return new S3BucketResponse(bucketName, sdkHttpResponse.statusCode(), null);
        } catch (final S3Exception s3Exception) {
            final int statusCode = s3Exception.awsErrorDetails().sdkHttpResponse().statusCode();
            return new S3BucketResponse(bucketName, statusCode, s3Exception);
        }
    }

    public void saveObject(final @NotNull String objectKey, final @NotNull String content) {
        Objects.requireNonNull(s3Client)
                .putObject(builder -> builder.bucket(Objects.requireNonNull(s3Config).getBucketName())
                        .key(objectKey)
                        .build(), RequestBody.fromString(content));
    }

    public void deleteObject(final @NotNull String objectKey) {
        Objects.requireNonNull(s3Client)
                .deleteObject(builder -> builder.bucket(Objects.requireNonNull(s3Config).getBucketName())
                        .key(objectKey)
                        .build());
    }

    public @NotNull String getObject(final @NotNull String objectKey) {
        return Objects.requireNonNull(s3Client)
                .getObjectAsBytes(builder -> builder.bucket(Objects.requireNonNull(s3Config).getBucketName())
                        .key(objectKey)
                        .build())
                .asUtf8String();
    }

    public @NotNull ListObjectsV2Response getObjects() {
        return Objects.requireNonNull(s3Client)
                .listObjectsV2(builder -> builder.bucket(Objects.requireNonNull(s3Config).getBucketName())
                        .prefix(s3Config.getFilePrefix())
                        .build());
    }

    public @NotNull ListObjectsV2Response getNextBatchOfObjects(final @NotNull String continuationToken) {
        return Objects.requireNonNull(s3Client)
                .listObjectsV2(builder -> builder.bucket(Objects.requireNonNull(s3Config).getBucketName())
                        .prefix(s3Config.getFilePrefix())
                        .continuationToken(continuationToken)
                        .build());
    }

    public void setS3Client(final @NotNull S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Nullable S3Client getS3Client() {
        return s3Client;
    }

    public @Nullable S3Config getS3Config() {
        return s3Config;
    }
}
