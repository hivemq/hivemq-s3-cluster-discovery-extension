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

    private static final String S3_CONFIG_FILE = "s3discovery.properties";

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
            logger.debug("Read properties file '{}' successfully.", propertiesFile.getAbsolutePath());
            return s3Config;

        } catch (final IOException ex) {
            logger.error("An error occurred while reading the properties file {}", propertiesFile.getAbsolutePath(), ex);
        }

        return null;
    }
}