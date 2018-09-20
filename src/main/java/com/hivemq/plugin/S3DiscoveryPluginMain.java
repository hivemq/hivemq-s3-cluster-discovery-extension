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
import com.hivemq.plugin.configuration.Configuration;
import com.hivemq.plugin.configuration.PluginPropertiesReader;
import com.hivemq.plugin.amazon.S3Client;

/**
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class S3DiscoveryPluginMain implements PluginMain {

    //    private static final Logger log = LoggerFactory.getLogger(S3DiscoveryPluginMain.class);

    @Override
    public void pluginStart(@NotNull final PluginStartInput pluginStartInput, @NotNull final PluginStartOutput pluginStartOutput) {

        try {
            final PluginPropertiesReader pluginPropertiesReader = new PluginPropertiesReader(pluginStartInput.getPluginInformation());
            final Configuration configuration = new Configuration(pluginPropertiesReader);
            final S3Client s3Client = new S3Client(configuration);

            Services.clusterService().addDiscoveryCallback(new S3DiscoveryCallback(s3Client.get(), configuration));
        } catch (final Exception e){
//            log.error("Not able to start S3 Plugin: ", e);
        }

    }

    @Override
    public void pluginStop(@NotNull final PluginStopInput pluginStopInput, @NotNull final PluginStopOutput pluginStopOutput) {

    }
}
