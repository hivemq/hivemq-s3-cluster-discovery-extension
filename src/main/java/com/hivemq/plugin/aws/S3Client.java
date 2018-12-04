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

package com.hivemq.plugin.aws;

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
import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.api.annotations.Nullable;
import com.hivemq.plugin.config.AuthenticationType;
import com.hivemq.plugin.config.ConfigurationReader;
import com.hivemq.plugin.config.ConfigurationValidator;
import com.hivemq.plugin.config.S3Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3Client {

    private static Logger logger = LoggerFactory.getLogger(S3Client.class);

    private final S3Config s3Config;
    private final AmazonS3 amazonS3;

    private S3Client(@NotNull final S3Config s3Config) {

        this.s3Config = s3Config;

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
            amazonS3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Config.getEndpoint(), s3Config.getEndpointRegionName()));
        }

        this.amazonS3 = amazonS3ClientBuilder.build();
    }

    @Nullable
    public static S3Client create(@NotNull final ConfigurationReader configurationReader) {

        final S3Config s3Config = configurationReader.readConfiguration();
        if (s3Config == null) {
            logger.error("Configuration of the S3 discovery plugin couldn't be loaded.");
            return null;
        }

        final ConfigurationValidator configurationValidator = new ConfigurationValidator(s3Config);
        if (!configurationValidator.isValid()) {
            logger.error("Configuration of the S3 Discovery plugin is not valid!");
            return null;
        }

        final S3Client s3Client;
        try {
            s3Client = new S3Client(s3Config);
        } catch (final Exception ex) {
            logger.error("An exception occurred while creating the S3Client.", ex);
            return null;
        }
        if (!s3Client.doesBucketExist()) {
            logger.error("Configured bucket '{}' doesn't exist.", s3Config.getBucketName());
            return null;
        }

        logger.trace("Loaded configuration and created S3Client successfully.");

        return s3Client;
    }

    @NotNull
    public S3Config getS3Config() {
        return s3Config;
    }

    @NotNull
    private AWSCredentialsProvider getAwsCredentials(final AuthenticationType authenticationType) {

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
                if (accessKey == null || secretAccessKey == null) {
                    throw new NullPointerException("Access key or secret access key is null");
                }

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

    public boolean doesBucketExist() {

        final String bucketName = s3Config.getBucketName();

        try {
            return amazonS3.doesBucketExistV2(bucketName);
        } catch (final AmazonS3Exception ignored) {
            logger.trace("Caught an exception from S3 Discovery plugin while checking if bucket '{}' exists. Returning false.", bucketName);
            return false;
        }
    }

    public void saveObject(@NotNull final String objectKey, @NotNull final String content) throws Exception {

        final InputStream inputStream = new StringInputStream(content);

        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(inputStream.available());

        amazonS3.putObject(s3Config.getBucketName(), objectKey, inputStream, objectMetadata);
    }

    public void removeObject(@NotNull final String objectKey) {
        amazonS3.deleteObject(s3Config.getBucketName(), objectKey);
    }

    public ObjectListing getObjects(@NotNull final String filePrefix) {
        return amazonS3.listObjects(s3Config.getBucketName(), filePrefix);
    }

    public S3Object getObject(@NotNull final String key) {
        return amazonS3.getObject(s3Config.getBucketName(), key);
    }

    public ObjectListing getNextBatchOfObjects(@NotNull final ObjectListing objectListing) {
        return amazonS3.listNextBatchOfObjects(objectListing);
    }

    public void deleteObject(@NotNull final String key) {
        amazonS3.deleteObject(s3Config.getBucketName(), key);
    }
}
