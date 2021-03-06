/*
 * Copyright 2018 dc-square GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.aws;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringInputStream;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extensions.config.AuthenticationType;
import com.hivemq.extensions.config.ConfigurationReader;
import com.hivemq.extensions.config.S3Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Abdullah Imal
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class S3Client {

    private static final Logger logger = LoggerFactory.getLogger(S3Client.class);

    public @NotNull ConfigurationReader configurationReader;

    private @Nullable S3Config s3Config;

    public @Nullable AmazonS3 amazonS3;

    public S3Client(final @NotNull ConfigurationReader configurationReader) {
        this.configurationReader = configurationReader;
    }

    public void createOrUpdate() {

        final S3Config newS3Config = configurationReader.readConfiguration();
        if (newS3Config == null) {
            throw new IllegalStateException("Configuration of the S3 discovery extension couldn't be loaded.");
        }
        s3Config = newS3Config;
        logger.trace("Loaded configuration successfully.");

        final AuthenticationType authenticationType = AuthenticationType.fromName(s3Config.getAuthenticationTypeName());
        final AWSCredentialsProvider credentialsProvider = getAwsCredentials(authenticationType);
        final Regions regions = Regions.fromName(s3Config.getBucketRegionName());

        final AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider);

        if (s3Config.getPathStyleAccess() != null) {
            amazonS3ClientBuilder.withPathStyleAccessEnabled(s3Config.getPathStyleAccess());
        }

        if (s3Config.getEndpoint().contentEquals(Constants.S3_HOSTNAME)) {
            amazonS3ClientBuilder.withRegion(regions);
        } else {
            amazonS3ClientBuilder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(s3Config.getEndpoint(), s3Config.getEndpointRegionName())
            );
        }

        amazonS3 = amazonS3ClientBuilder.build();
        logger.trace("Created AmazonS3 Client successfully.");
    }

    public @NotNull S3Config getS3Config() {
        return s3Config;
    }

    @NotNull AWSCredentialsProvider getAwsCredentials(final @NotNull AuthenticationType authenticationType) {

        final AWSCredentialsProvider credentialsProvider;

        switch (authenticationType) {
            case DEFAULT:
                credentialsProvider = new DefaultAWSCredentialsProviderChain();
                break;
            case ENVIRONMENT_VARIABLES:
                credentialsProvider = new EnvironmentVariableCredentialsProvider();
                break;
            case JAVA_SYSTEM_PROPERTIES:
                credentialsProvider = new SystemPropertiesCredentialsProvider();
                break;
            case USER_CREDENTIALS_FILE:
                credentialsProvider = new ProfileCredentialsProvider();
                break;
            case INSTANCE_PROFILE_CREDENTIALS:
                credentialsProvider = new InstanceProfileCredentialsProvider(false);
                break;
            case ACCESS_KEY:
            case TEMPORARY_SESSION:
                final String accessKey = s3Config.getAccessKeyId();
                final String secretAccessKey = s3Config.getAccessKeySecret();

                if (authenticationType == AuthenticationType.ACCESS_KEY) {
                    credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretAccessKey));
                    break;
                }

                final String sessionToken = s3Config.getSessionToken();
                credentialsProvider = new AWSStaticCredentialsProvider(new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken));
                break;
            default:
                throw new IllegalArgumentException("Unknown credentials type.");
        }
        return credentialsProvider;
    }

    public boolean existsBucket() {
        final String bucketName = s3Config.getBucketName();

        try {
            return amazonS3.doesBucketExistV2(bucketName);
        } catch (final AmazonS3Exception ignored) {
            logger.trace("Caught an exception from S3 Discovery extension while checking if bucket '{}' exists. Returning false.", bucketName);
            return false;
        }
    }

    public void saveObject(final @NotNull String objectKey, final @NotNull String content) throws IOException {
        final InputStream inputStream = new StringInputStream(content);

        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(inputStream.available());

        amazonS3.putObject(s3Config.getBucketName(), objectKey, inputStream, objectMetadata);
    }

    public void deleteObject(final @NotNull String objectKey) {
        amazonS3.deleteObject(s3Config.getBucketName(), objectKey);
    }

    public S3Object getObject(final @NotNull String objectKey) {
        return amazonS3.getObject(s3Config.getBucketName(), objectKey);
    }

    public ObjectListing getObjects(final @NotNull String filePrefix) {
        return amazonS3.listObjects(s3Config.getBucketName(), filePrefix);
    }

    public ObjectListing getNextBatchOfObjects(final @NotNull ObjectListing objectListing) {
        return amazonS3.listNextBatchOfObjects(objectListing);
    }
}
