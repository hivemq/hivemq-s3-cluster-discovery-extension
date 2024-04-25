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

package com.hivemq.extensions.cluster.discovery.s3;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extensions.cluster.discovery.s3.config.ConfigurationReader;
import com.hivemq.extensions.cluster.discovery.s3.logging.ExtensionLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public class S3DiscoveryExtensionMain implements ExtensionMain {

    private static final @NotNull Logger LOG = LoggerFactory.getLogger(S3DiscoveryExtensionMain.class);

    private final @NotNull ExtensionLogging extensionLogging;
    private final @NotNull S3DiscoveryMetrics s3DiscoveryMetrics;
    @Nullable S3DiscoveryCallback s3DiscoveryCallback;

    @SuppressWarnings("unused")
    public S3DiscoveryExtensionMain() {
        this(new ExtensionLogging(), new S3DiscoveryMetrics(Services.metricRegistry()));
    }

    S3DiscoveryExtensionMain(
            final @NotNull ExtensionLogging extensionLogging, final @NotNull S3DiscoveryMetrics s3DiscoveryMetrics) {
        this.extensionLogging = extensionLogging;
        this.s3DiscoveryMetrics = s3DiscoveryMetrics;
    }

    @Override
    public void extensionStart(
            final @NotNull ExtensionStartInput extensionStartInput,
            final @NotNull ExtensionStartOutput extensionStartOutput) {
        try {
            extensionLogging.start();
            final ConfigurationReader configurationReader =
                    new ConfigurationReader(extensionStartInput.getExtensionInformation());
            s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader, s3DiscoveryMetrics);

            Services.clusterService().addDiscoveryCallback(s3DiscoveryCallback);
            LOG.debug("{}: Registered S3 discovery callback successfully.", ExtensionConstants.EXTENSION_NAME);
        } catch (final UnsupportedOperationException e) {
            extensionStartOutput.preventExtensionStartup(e.getMessage());
        } catch (final Exception e) {
            LOG.error("{}: Exception thrown at extension start: ", ExtensionConstants.EXTENSION_NAME, e);
            extensionStartOutput.preventExtensionStartup("Exception thrown");
        }
    }

    @Override
    public void extensionStop(
            final @NotNull ExtensionStopInput extensionStopInput,
            final @NotNull ExtensionStopOutput extensionStopOutput) {
        if (s3DiscoveryCallback != null) {
            Services.clusterService().removeDiscoveryCallback(s3DiscoveryCallback);
        }
        extensionLogging.stop();
        s3DiscoveryMetrics.stop();
    }
}
