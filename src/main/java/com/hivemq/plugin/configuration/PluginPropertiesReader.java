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

import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.api.parameter.PluginInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class PluginPropertiesReader {

    private static final Logger log = LoggerFactory.getLogger(PluginPropertiesReader.class);

    private final @NotNull Properties properties = new Properties();

    public PluginPropertiesReader(final @NotNull PluginInformation pluginInformation) {

        final File configFolder = pluginInformation.getPluginHomeFolder();

        final File pluginFile = new File(configFolder, "s3discovery.properties");

        if (!pluginFile.canRead()) {
            log.error("Could not read the properties file {}", pluginFile.getAbsolutePath());
            return;
        }

        try (final InputStream is = new FileInputStream(pluginFile)) {
            log.debug("Reading property file {}", pluginFile.getAbsolutePath());
            properties.load(is);

        } catch (final IOException e) {
            log.error("An error occurred while reading the properties file {}", pluginFile.getAbsolutePath(), e);
        }
    }

    @NotNull Properties getProperties() {
        return properties;
    }

}