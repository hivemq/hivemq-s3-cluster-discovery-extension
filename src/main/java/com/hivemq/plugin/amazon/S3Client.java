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

package com.hivemq.plugin.amazon;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.configuration.AuthenticationType;
import com.hivemq.plugin.configuration.Configuration;

import java.util.Map;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class S3Client {

    //FIXME: add logger and logstatements
//    private static final Logger log = LoggerFactory.getLogger(S3Client.class);

    @NotNull private final Configuration configuration;

    public S3Client(@NotNull final Configuration configuration) {
        this.configuration = configuration;
    }

    public @NotNull AmazonS3 get() {

        final AuthenticationType authenticationType = configuration.getAuthenticationType();
        if (authenticationType == null) {
//            log.error("S3 credentials type not configured, shutting down HiveMQ");
            throw new NullPointerException("S3 Authentication type is null");
        }

        final AWSCredentialsProvider credentialsProvider;
        try {
            credentialsProvider = getAwsCredentials(authenticationType);
        } catch (final Exception e) {
//            log.error("Not able to authenticate with S3, shutting down HiveMQ");
            throw e;
        }

        final AmazonS3 s3;
        try {
            s3 = new AmazonS3Client(credentialsProvider);
        } catch (Exception e) {
//            log.error("Not able to authenticate with S3, shutting down HiveMQ");
            throw e;
        }

        final Regions regions = configuration.getRegion();
        if (regions == null) {
//            log.error("S3 region is not configured, shutting down HiveMQ");
            throw new NullPointerException("S3 region is null");
        }

        System.out.println("Region: " + regions.getName());

        final Region region = Region.getRegion(regions);
        s3.setRegion(region);

        final String bucketName = configuration.getBucketName();

        if (bucketName == null) {
//            log.error("S3 Bucket name is not configured, shutting down HiveMQ");
            throw new NullPointerException("S3 Bucket name is null");
        }

        try {

            if (!s3.doesBucketExist(bucketName)) {
//                log.error("S3 Bucket {} does not exist, shutting down HiveMQ", bucketName);
                throw new IllegalArgumentException("S3 Bucket does not exist");
            }
        } catch (final AmazonS3Exception e) {
            for (final Map.Entry<String, String> entry : e.getAdditionalDetails().entrySet()) {
//                log.debug("Additional Error information {} : {}", entry.getKey(), entry.getValue());
            }
//            log.error("Error at checking if S3 bucket {} exists", bucketName, e);
        }

        return s3;
    }

    private @NotNull AWSCredentialsProvider getAwsCredentials(final AuthenticationType authenticationType) {
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
                credentialsProvider = new InstanceProfileCredentialsProvider();
                break;
            case ACCESS_KEY:
            case TEMPORARY_SESSION:
                final String accessKey = configuration.getAccessKeyId();
                final String secretAccessKey = configuration.getSecretAccessKey();
                if (authenticationType == AuthenticationType.ACCESS_KEY) {
                    credentialsProvider = new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretAccessKey));
                    break;
                }

                final String sessionToken = configuration.getSessionToken();
                credentialsProvider = new StaticCredentialsProvider(new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken));
                break;
            default:
                throw new IllegalArgumentException("Unknown credentials type");
        }
        return credentialsProvider;
    }

}
