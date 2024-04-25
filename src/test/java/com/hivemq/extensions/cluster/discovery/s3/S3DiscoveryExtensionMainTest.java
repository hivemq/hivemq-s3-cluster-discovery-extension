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

import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
import com.hivemq.extensions.cluster.discovery.s3.logging.ExtensionLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3DiscoveryExtensionMainTest {

    private @NotNull ExtensionStartInput extensionStartInput;
    private @NotNull ExtensionStartOutput extensionStartOutput;
    private @NotNull ExtensionStopInput extensionStopInput;
    private @NotNull ExtensionStopOutput extensionStopOutput;
    private @NotNull ExtensionInformation extensionInformation;
    private @NotNull S3DiscoveryExtensionMain s3DiscoveryExtensionMain;

    @BeforeEach
    void setUp(@TempDir final @NotNull File tempDir) {
        extensionStartInput = mock(ExtensionStartInput.class);
        extensionStartOutput = mock(ExtensionStartOutput.class);
        extensionStopInput = mock(ExtensionStopInput.class);
        extensionStopOutput = mock(ExtensionStopOutput.class);
        extensionInformation = mock(ExtensionInformation.class);
        when(extensionStartInput.getExtensionInformation()).thenReturn(extensionInformation);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir);
        s3DiscoveryExtensionMain =
                new S3DiscoveryExtensionMain(mock(ExtensionLogging.class), mock(S3DiscoveryMetrics.class));
    }

    @Test
    void test_start_success() {
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        assertNotNull(s3DiscoveryExtensionMain.s3DiscoveryCallback);
    }

    @Test
    void test_start_failed() {
        when(extensionInformation.getExtensionHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        assertNull(s3DiscoveryExtensionMain.s3DiscoveryCallback);
    }

    @Test
    void test_stop_success() {
        assertThrows(RuntimeException.class, () -> {
            s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
            s3DiscoveryExtensionMain.extensionStop(extensionStopInput, extensionStopOutput);
        });
    }

    @Test
    void test_stop_no_start_failed() {
        when(extensionInformation.getExtensionHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        s3DiscoveryExtensionMain.extensionStop(extensionStopInput, extensionStopOutput);
        assertNull(s3DiscoveryExtensionMain.s3DiscoveryCallback);
    }
}
