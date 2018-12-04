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

package com.hivemq.plugin;

import com.hivemq.plugin.api.PluginMain;
import com.hivemq.plugin.api.annotations.NotNull;
import com.hivemq.plugin.api.parameter.PluginStartInput;
import com.hivemq.plugin.api.parameter.PluginStartOutput;
import com.hivemq.plugin.api.parameter.PluginStopInput;
import com.hivemq.plugin.api.parameter.PluginStopOutput;
import com.hivemq.plugin.api.services.Services;
import com.hivemq.plugin.callbacks.S3DiscoveryCallback;
import com.hivemq.plugin.config.ConfigurationReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryPluginMain implements PluginMain {

    private static final Logger logger = LoggerFactory.getLogger(S3DiscoveryPluginMain.class);

    private S3DiscoveryCallback s3DiscoveryCallback;

    @Override
    public void pluginStart(@NotNull final PluginStartInput pluginStartInput, @NotNull final PluginStartOutput pluginStartOutput) {
        try {
            final ConfigurationReader configurationReader = new ConfigurationReader(pluginStartInput.getPluginInformation());

            s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);

            Services.clusterService().addDiscoveryCallback(s3DiscoveryCallback);

            logger.debug("Registered S3 discovery callback successfully.");
        } catch (final Exception ex) {
            logger.error("Not able to start S3 Discovery Plugin.", ex);
            pluginStartOutput.preventPluginStartup("Exception caught at plugin start.");
        }
    }

    @Override
    public void pluginStop(@NotNull final PluginStopInput pluginStopInput, @NotNull final PluginStopOutput pluginStopOutput) {

        if (s3DiscoveryCallback != null) {
            Services.clusterService().removeDiscoveryCallback(s3DiscoveryCallback);
        }
    }
}
