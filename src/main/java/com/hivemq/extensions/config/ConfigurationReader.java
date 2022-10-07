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

package com.hivemq.extensions.config;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hivemq.extensions.util.StringUtil.isNullOrBlank;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class ConfigurationReader {

    public static final @NotNull String S3_CONFIG_FILE = "s3discovery.properties";

    private static final @NotNull Logger logger = LoggerFactory.getLogger(ConfigurationReader.class);

    private final @NotNull File extensionHomeFolder;

    public ConfigurationReader(final @NotNull ExtensionInformation extensionInformation) {
        extensionHomeFolder = extensionInformation.getExtensionHomeFolder();
    }

    public @Nullable S3Config readConfiguration() {
        final File propertiesFile = new File(extensionHomeFolder, S3_CONFIG_FILE);

        if (!propertiesFile.exists()) {
            logger.error("Could not find '{}'. Please verify that the properties file is located under '{}'.",
                    S3_CONFIG_FILE,
                    extensionHomeFolder);
            return null;
        }

        if (!propertiesFile.canRead()) {
            logger.error(
                    "Could not read '{}'. Please verify that the user running HiveMQ has reading permissions for it.",
                    propertiesFile.getAbsolutePath());
            return null;
        }

        try (final InputStream inputStream = new FileInputStream(propertiesFile)) {
            logger.debug("Reading properties file '{}'.", propertiesFile.getAbsolutePath());
            final Properties properties = new Properties();
            properties.load(inputStream);

            final S3Config s3Config = ConfigFactory.create(S3Config.class, properties);
            if (!isValid(s3Config)) {
                logger.error("Configuration of the S3 Discovery extension is not valid!");
                return null;
            }
            logger.trace("Read properties file '{}' successfully.", propertiesFile.getAbsolutePath());
            return s3Config;

        } catch (final FileNotFoundException e) {
            logger.error("Could not find the properties file '{}'", propertiesFile.getAbsolutePath());
        } catch (final IOException e) {
            logger.error("An error occurred while reading the properties file {}", propertiesFile.getAbsolutePath(), e);
        }
        return null;
    }

    private static boolean isValid(final @NotNull S3Config s3Config) {
        final String bucketName = s3Config.getBucketName();
        if (isNullOrBlank(bucketName)) {
            logger.error("S3 Discovery Extension - Bucket name is empty!");
            return false;
        }

        final String bucketRegionName = s3Config.getBucketRegionName();
        if (isNullOrBlank(bucketRegionName)) {
            logger.error("S3 Discovery Extension - Bucket region is empty!");
            return false;
        }

        if (Region.regions().stream().noneMatch(region -> region.id().equals(bucketRegionName))) {
            logger.error("S3 Discovery Extension - Given bucket region is not a valid region!");
            return false;
        }

        final String authenticationTypeName = s3Config.getAuthenticationTypeName();
        if (isNullOrBlank(authenticationTypeName)) {
            logger.error("S3 Discovery Extension - Authentication type is empty!");
            return false;
        }

        final AuthenticationType authenticationType;
        try {
            authenticationType = AuthenticationType.fromName(authenticationTypeName);
        } catch (final IllegalArgumentException ignored) {
            logger.error("S3 Discovery Extension - Given authentication type is not valid!");
            return false;
        }

        if (authenticationType == AuthenticationType.ACCESS_KEY ||
                authenticationType == AuthenticationType.TEMPORARY_SESSION) {
            final String accessKeyId = s3Config.getAccessKeyId();
            if (isNullOrBlank(accessKeyId)) {
                logger.error("S3 Discovery Extension - Access key id is empty!");
                return false;
            }

            final String accessKeySecret = s3Config.getAccessKeySecret();
            if (isNullOrBlank(accessKeySecret)) {
                logger.error("S3 Discovery Extension - Access key secret is empty!");
                return false;
            }

            if (authenticationType == AuthenticationType.TEMPORARY_SESSION) {
                final String sessionToken = s3Config.getSessionToken();
                if (isNullOrBlank(sessionToken)) {
                    logger.error("S3 Discovery Extension - Session token is empty!");
                    return false;
                }
            }
        }

        final long fileExpirationInSeconds;
        try {
            fileExpirationInSeconds = s3Config.getFileExpirationInSeconds();
        } catch (final UnsupportedOperationException e) {
            logger.error("S3 Discovery Extension - File expiration interval is not set!");
            return false;
        }
        if (fileExpirationInSeconds < 0) {
            logger.error("S3 Discovery Extension - File expiration interval is negative!");
            return false;
        }

        final long fileUpdateIntervalInSeconds;
        try {
            fileUpdateIntervalInSeconds = s3Config.getFileUpdateIntervalInSeconds();
        } catch (final UnsupportedOperationException e) {
            logger.error("S3 Discovery Extension - File update interval is not set!");
            return false;
        }
        if (fileUpdateIntervalInSeconds < 0) {
            logger.error("S3 Discovery Extension - File update interval is negative!");
            return false;
        }

        if (!(fileUpdateIntervalInSeconds == 0 && fileExpirationInSeconds == 0)) {
            if (fileUpdateIntervalInSeconds == fileExpirationInSeconds) {
                logger.error("S3 Discovery Extension - File update interval is the same as the expiration interval!");
                return false;
            }

            if (fileUpdateIntervalInSeconds == 0) {
                logger.error("S3 Discovery Extension - File update interval is deactivated but expiration is set!");
                return false;
            }

            if (fileExpirationInSeconds == 0) {
                logger.error("S3 Discovery Extension - File expiration is deactivated but update interval is set!");
                return false;
            }

            if (!(fileUpdateIntervalInSeconds < fileExpirationInSeconds)) {
                logger.error("S3 Discovery Extension - File update interval is larger than expiration interval!");
                return false;
            }
        }
        return true;
    }
}
