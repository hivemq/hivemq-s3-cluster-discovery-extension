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
import com.hivemq.plugin.api.annotations.Nullable;
import com.hivemq.plugin.api.parameter.PluginInformation;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class ConfigurationReader {

    public static final String S3_CONFIG_FILE = "s3discovery.properties";

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationReader.class);

    private final File pluginHomeFolder;

    public ConfigurationReader(@NotNull final PluginInformation pluginInformation) {
        this.pluginHomeFolder = pluginInformation.getPluginHomeFolder();
    }

    @Nullable
    public S3Config readConfiguration() {
        final File propertiesFile = new File(pluginHomeFolder, S3_CONFIG_FILE);

        if (!propertiesFile.exists()) {
            logger.error("Could not find '{}'. Please verify that the properties file is located under '{}'.", S3_CONFIG_FILE, pluginHomeFolder);
            return null;
        }

        if (!propertiesFile.canRead()) {
            logger.error("Could not read '{}'. Please verify that the user running HiveMQ has reading permissions for it.", propertiesFile.getAbsolutePath());
            return null;
        }

        try (final InputStream inputStream = new FileInputStream(propertiesFile)) {

            logger.debug("Reading properties file '{}'.", propertiesFile.getAbsolutePath());
            final Properties properties = new Properties();
            properties.load(inputStream);

            final S3Config s3Config = ConfigFactory.create(S3Config.class, properties);
            if (!isValid(s3Config)) {
                logger.error("Configuration of the S3 Discovery plugin is not valid!");
                return null;
            }
            logger.debug("Read properties file '{}' successfully.", propertiesFile.getAbsolutePath());
            return s3Config;

        } catch (final IOException ex) {
            logger.error("An error occurred while reading the properties file {}", propertiesFile.getAbsolutePath(), ex);
        }

        return null;
    }

    private boolean isValid(@NotNull final S3Config s3Config) {
        final String bucketName = s3Config.getBucketName();
        if (bucketName == null || bucketName.isBlank()) {
            logger.error("S3 Discovery Plugin - Bucket name is empty!");
            return false;
        }

        final String bucketRegionName = s3Config.getBucketRegionName();
        if (bucketRegionName == null || bucketRegionName.isEmpty()) {
            logger.error("S3 Discovery Plugin - Bucket region is empty!");
            return false;
        }

        try {
            Regions.fromName(bucketRegionName);
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

        final Long fileExpirationInSeconds;
        try {
            fileExpirationInSeconds = s3Config.getFileExpirationInSeconds();
        } catch (final UnsupportedOperationException ex) {
            logger.error("S3 Discovery Plugin - File expiration interval is not set!");
            return false;
        }
        if (fileExpirationInSeconds < 0) {
            logger.error("S3 Discovery Plugin - File expiration interval is negative!");
            return false;
        }

        final Long fileUpdateIntervalInSeconds;
        try {
            fileUpdateIntervalInSeconds = s3Config.getFileUpdateIntervalInSeconds();
        } catch (final UnsupportedOperationException ex) {
            logger.error("S3 Discovery Plugin - File update interval is not set!");
            return false;
        }
        if (fileUpdateIntervalInSeconds < 0) {
            logger.error("S3 Discovery Plugin - File update interval is negative!");
            return false;
        }

        if (!(fileUpdateIntervalInSeconds == 0 && fileExpirationInSeconds == 0)) {

            if (fileUpdateIntervalInSeconds.equals(fileExpirationInSeconds)) {
                logger.error("S3 Discovery Plugin - File update interval is the same as the expiration interval!");
                return false;
            }

            if (fileUpdateIntervalInSeconds == 0) {
                logger.error("S3 Discovery Plugin - File update interval is deactivated but expiration is set!");
                return false;
            }

            if (fileExpirationInSeconds == 0) {
                logger.error("S3 Discovery Plugin - File expiration is deactivated but update interval is set!");
                return false;
            }

            if (!(fileUpdateIntervalInSeconds < fileExpirationInSeconds)) {
                logger.error("S3 Discovery Plugin - File update interval is larger than expiration interval!");
                return false;
            }
        }

        return true;
    }
}