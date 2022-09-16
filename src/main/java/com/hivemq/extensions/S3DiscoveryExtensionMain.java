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

package com.hivemq.extensions;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extensions.callbacks.S3DiscoveryCallback;
import com.hivemq.extensions.config.ConfigurationReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryExtensionMain implements ExtensionMain {

    private static final @NotNull Logger logger = LoggerFactory.getLogger(S3DiscoveryExtensionMain.class);

    @Nullable S3DiscoveryCallback s3DiscoveryCallback;

    @Override
    public void extensionStart(
            final @NotNull ExtensionStartInput extensionStartInput,
            final @NotNull ExtensionStartOutput extensionStartOutput) {

        try {
            final ConfigurationReader configurationReader =
                    new ConfigurationReader(extensionStartInput.getExtensionInformation());
            s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader);

            Services.clusterService().addDiscoveryCallback(s3DiscoveryCallback);
            logger.debug("Registered S3 discovery callback successfully.");
        } catch (final UnsupportedOperationException e) {
            extensionStartOutput.preventExtensionStartup(e.getMessage());
        } catch (final Exception e) {
            logger.error("Not able to start S3 Discovery Extension.", e);
            extensionStartOutput.preventExtensionStartup("Exception caught at extension start.");
        }
    }

    @Override
    public void extensionStop(
            final @NotNull ExtensionStopInput extensionStopInput,
            final @NotNull ExtensionStopOutput extensionStopOutput) {
        if (s3DiscoveryCallback != null) {
            Services.clusterService().removeDiscoveryCallback(s3DiscoveryCallback);
        }
    }
}
