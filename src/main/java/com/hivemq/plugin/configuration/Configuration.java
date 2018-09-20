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

package com.hivemq.plugin.configuration;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.internal.Constants;
import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.api.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class Configuration {

    private static final Logger log = LoggerFactory.getLogger(Configuration.class);

    private @NotNull final Properties properties;

    public Configuration(final @NotNull PluginPropertiesReader pluginPropertiesReader) {
        properties = pluginPropertiesReader.getProperties();
    }


    public @Nullable AuthenticationType getAuthenticationType() {

        try {
            final String property = getProperty("credentials-type");
            if (property == null) {
                return null;
            }

            return AuthenticationType.fromName(property);
        } catch (final IllegalArgumentException e) {
            log.error("Not able to initialize S3 Plugin", e);
            return null;
        }
    }

    public @Nullable Regions getRegion() {
        try {
            final String property = getProperty("s3-bucket-region");
            if (property == null) {
                return null;
            }

            return Regions.fromName(property);
        } catch (final IllegalArgumentException e) {
            log.error("Not able to initialize S3 Plugin", e);
            return null;
        }
    }

    public @Nullable String getFilePrefix() {
        final String property;

        if (System.getenv("S3_FILE_PREFIX") != null) {
            property = System.getenv("S3_FILE_PREFIX");
        } else if (getProperty("file-prefix") != null) {
            property = getProperty("file-prefix");
        } else {
            property = "";
        }

        return property;
    }

    public long getExpirationMinutes() {
        final String property = getProperty("file-expiration");
        if (property == null) {
            return 0L;
        }

        try {
            final long value = Long.parseLong(property);
            if (value < 0) {
                log.error("Value for S3 expire configuration must be positive or zero, disabling expiration");
                return 0;
            }
            return value;
        } catch (final NumberFormatException e) {
            log.error("Not able to parse S3 expiration configuration, disabling expiration");
            return 0L;
        }
    }

    //FIXME: Use method for schedule task
    public long getOwnInformationUpdateInterval() {
        final String property = getProperty("update-interval");
        if (property == null) {
            return 0L;
        }

        try {
            final long value = Long.parseLong(property);
            if (value < 0) {
                log.error("Value for S3 update interval configuration must be positive or zero, disabling update interval");
                return 0;
            }
            return value;
        } catch (final NumberFormatException e) {
            log.error("Not able to parse S3 update interval configuration, disabling update interval");
            return 0L;
        }
    }

    public @Nullable String getBucketName() {
        return getProperty("s3-bucket-name");
    }

    public @Nullable String getAccessKeyId() {
        return getProperty("credentials-access-key-id");
    }

    public @Nullable String getSecretAccessKey() {
        return getProperty("credentials-secret-access-key");
    }

    public @Nullable String getSessionToken() {
        return getProperty("credentials-session-token");
    }

    public @Nullable String getEndpoint() {
        final String property = getProperty("s3-endpoint");
        return Objects.requireNonNullElse(property, Constants.S3_HOSTNAME);
    }

    public boolean withPathStyleAccess() {
        return Boolean.parseBoolean(getProperty("s3-path-style-access"));
    }

    private @Nullable String getProperty(final @NotNull String key) {
        return properties.getProperty(key);
    }
}
