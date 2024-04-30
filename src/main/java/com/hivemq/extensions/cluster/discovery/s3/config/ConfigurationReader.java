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

package com.hivemq.extensions.cluster.discovery.s3.config;

import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import org.aeonbits.owner.ConfigFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_CONFIGURATION;
import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_NAME;
import static com.hivemq.extensions.cluster.discovery.s3.util.StringUtil.isNullOrBlank;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class ConfigurationReader {

    private static final @NotNull Logger LOG = LoggerFactory.getLogger(ConfigurationReader.class);

    private final @NotNull File extensionHomeFolder;

    public ConfigurationReader(final @NotNull ExtensionInformation extensionInformation) {
        extensionHomeFolder = extensionInformation.getExtensionHomeFolder();
    }

    public @Nullable S3Config readConfiguration() {
        final File propertiesFile = new File(extensionHomeFolder, EXTENSION_CONFIGURATION);

        if (!propertiesFile.exists()) {
            LOG.error("{}: Could not find '{}'. Please verify that the properties file is located under '{}'.",
                    EXTENSION_NAME,
                    EXTENSION_CONFIGURATION,
                    extensionHomeFolder);
            return null;
        }

        if (!propertiesFile.canRead()) {
            LOG.error(
                    "{}: Could not read '{}'. Please verify that the user running HiveMQ has reading permissions for it.",
                    EXTENSION_NAME,
                    propertiesFile.getAbsolutePath());
            return null;
        }

        try (final InputStream inputStream = new FileInputStream(propertiesFile)) {
            LOG.debug("{}: Reading properties file '{}'.", EXTENSION_NAME, propertiesFile.getAbsolutePath());
            final Properties properties = new Properties();
            properties.load(inputStream);

            final S3Config s3Config = ConfigFactory.create(S3Config.class, properties);
            if (!isValid(s3Config)) {
                LOG.error("{}: Configuration is not valid!", EXTENSION_NAME);
                return null;
            }
            LOG.trace("{}: Read properties file '{}' successfully.", EXTENSION_NAME, propertiesFile.getAbsolutePath());
            return s3Config;

        } catch (final FileNotFoundException e) {
            LOG.error("{}: Could not find the properties file '{}'", EXTENSION_NAME, propertiesFile.getAbsolutePath());
        } catch (final IOException e) {
            LOG.error("{}: An error occurred while reading the properties file {}",
                    EXTENSION_NAME,
                    propertiesFile.getAbsolutePath(),
                    e);
        }
        return null;
    }

    private static boolean isValid(final @NotNull S3Config s3Config) {
        final String bucketName = s3Config.getBucketName();
        if (isNullOrBlank(bucketName)) {
            LOG.error("{}: Bucket name is empty!", EXTENSION_NAME);
            return false;
        }

        final String bucketRegionName = s3Config.getBucketRegionName();
        if (isNullOrBlank(bucketRegionName)) {
            LOG.error("{}: Bucket region is empty!", EXTENSION_NAME);
            return false;
        }

        if (Region.regions().stream().noneMatch(region -> region.id().equals(bucketRegionName))) {
            LOG.error("{}: Given bucket region is not a valid region!", EXTENSION_NAME);
            return false;
        }

        final String authenticationTypeName = s3Config.getAuthenticationTypeName();
        if (isNullOrBlank(authenticationTypeName)) {
            LOG.error("{}: Authentication type is empty!", EXTENSION_NAME);
            return false;
        }

        final AuthenticationType authenticationType;
        try {
            authenticationType = AuthenticationType.fromName(authenticationTypeName);
        } catch (final IllegalArgumentException ignored) {
            LOG.error("{}: Given authentication type is not valid!", EXTENSION_NAME);
            return false;
        }

        if (authenticationType == AuthenticationType.ACCESS_KEY ||
                authenticationType == AuthenticationType.TEMPORARY_SESSION) {
            final String accessKeyId = s3Config.getAccessKeyId();
            if (isNullOrBlank(accessKeyId)) {
                LOG.error("{}: Access key id is empty!", EXTENSION_NAME);
                return false;
            }

            final String accessKeySecret = s3Config.getAccessKeySecret();
            if (isNullOrBlank(accessKeySecret)) {
                LOG.error("{}: Access key secret is empty!", EXTENSION_NAME);
                return false;
            }

            if (authenticationType == AuthenticationType.TEMPORARY_SESSION) {
                final String sessionToken = s3Config.getSessionToken();
                if (isNullOrBlank(sessionToken)) {
                    LOG.error("{}: Session token is empty!", EXTENSION_NAME);
                    return false;
                }
            }
        }

        final long fileExpirationInSeconds;
        try {
            fileExpirationInSeconds = s3Config.getFileExpirationInSeconds();
        } catch (final UnsupportedOperationException e) {
            LOG.error("{}: File expiration interval is not set!", EXTENSION_NAME);
            return false;
        }
        if (fileExpirationInSeconds < 0) {
            LOG.error("{}: File expiration interval is negative!", EXTENSION_NAME);
            return false;
        }

        final long fileUpdateIntervalInSeconds;
        try {
            fileUpdateIntervalInSeconds = s3Config.getFileUpdateIntervalInSeconds();
        } catch (final UnsupportedOperationException e) {
            LOG.error("{}: File update interval is not set!", EXTENSION_NAME);
            return false;
        }
        if (fileUpdateIntervalInSeconds < 0) {
            LOG.error("{}: File update interval is negative!", EXTENSION_NAME);
            return false;
        }

        if (!(fileUpdateIntervalInSeconds == 0 && fileExpirationInSeconds == 0)) {
            if (fileUpdateIntervalInSeconds == fileExpirationInSeconds) {
                LOG.error("{}: File update interval is the same as the expiration interval!", EXTENSION_NAME);
                return false;
            }

            if (fileUpdateIntervalInSeconds == 0) {
                LOG.error("{}: File update interval is deactivated but expiration is set!", EXTENSION_NAME);
                return false;
            }

            if (fileExpirationInSeconds == 0) {
                LOG.error("{}: File expiration is deactivated but update interval is set!", EXTENSION_NAME);
                return false;
            }

            if (!(fileUpdateIntervalInSeconds < fileExpirationInSeconds)) {
                LOG.error("{}: File update interval is larger than expiration interval!", EXTENSION_NAME);
                return false;
            }
        }
        return true;
    }
}
