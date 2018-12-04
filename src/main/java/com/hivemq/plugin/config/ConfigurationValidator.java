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

package com.hivemq.plugin.config;

import com.amazonaws.regions.Regions;
import com.hivemq.plugin.api.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class ConfigurationValidator {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationValidator.class);

    private final S3Config s3Config;

    public ConfigurationValidator(@NotNull final S3Config s3Config) {
        this.s3Config = s3Config;
    }

    public boolean isValid() {

        final String bucketName = s3Config.getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            logger.error("S3 Discovery Plugin - Bucket name is empty!");
            return false;
        }

        final String bucketRegionName = s3Config.getBucketRegionName();
        if (bucketRegionName == null || bucketRegionName.isEmpty()) {
            logger.error("S3 Discovery Plugin - Bucket region is empty!");
            return false;
        }

        final Regions bucketRegion;
        try {
            bucketRegion = Regions.fromName(bucketRegionName);
        } catch (final IllegalArgumentException ignored) {
            logger.error("S3 Discovery Plugin - Given bucket region is not a valid region!");
            return false;
        }

        final String authenticationTypeName = s3Config.getAuthenticationTypeName();
        if (authenticationTypeName == null || authenticationTypeName.isEmpty()) {
            logger.error("S3 Discovery Plugin - Authentication type is empty!");
            return false;
        }

        final AuthenticationType authenticationType;
        try {
            authenticationType = AuthenticationType.fromName(authenticationTypeName);
        } catch (final IllegalArgumentException ignored) {
            logger.error("S3 Discovery Plugin - Given authentication type is not valid!");
            return false;
        }

        final Long fileExpirationInSeconds = s3Config.getFileExpirationInSeconds();
        if (fileExpirationInSeconds == null || fileExpirationInSeconds < 0) {
            logger.error("S3 Discovery Plugin - File expiration is not set or negative!");
            return false;
        }

        final Long fileUpdateIntervalInSeconds = s3Config.getFileUpdateIntervalInSeconds();
        if (fileUpdateIntervalInSeconds == null || fileUpdateIntervalInSeconds < 0) {
            logger.error("S3 Discovery Plugin - File update interval is not set or negative!");
            return false;
        }

        if (!(fileUpdateIntervalInSeconds == 0 && fileExpirationInSeconds == 0) && !(fileUpdateIntervalInSeconds < fileExpirationInSeconds)) {
            logger.error("S3 Discovery Plugin - File update interval is larger than expiration interval!");
            return false;
        }

        if (authenticationType == AuthenticationType.ACCESS_KEY || authenticationType == AuthenticationType.TEMPORARY_SESSION) {

            final String accessKeyId = s3Config.getAccessKeyId();
            if (accessKeyId == null || accessKeyId.isEmpty()) {
                logger.error("S3 Discovery Plugin - Access key id is empty!");
                return false;
            }

            final String accessKeySecret = s3Config.getAccessKeySecret();
            if (accessKeySecret == null || accessKeySecret.isEmpty()) {
                logger.error("S3 Discovery Plugin - Access key secret is empty!");
                return false;
            }

            if (authenticationType == AuthenticationType.TEMPORARY_SESSION) {
                final String sessionToken = s3Config.getSessionToken();
                if (sessionToken == null || sessionToken.isEmpty()) {
                    logger.error("S3 Discovery Plugin - Session token is empty!");
                    return false;
                }
            }
        }

        return true;
    }
}
