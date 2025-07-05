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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3DiscoveryExtensionMainTest {

    private final @NotNull ExtensionStartInput extensionStartInput = mock();
    private final @NotNull ExtensionStartOutput extensionStartOutput = mock();
    private final @NotNull ExtensionStopInput extensionStopInput = mock();
    private final @NotNull ExtensionStopOutput extensionStopOutput = mock();
    private final @NotNull ExtensionInformation extensionInformation = mock();

    private final @NotNull S3DiscoveryExtensionMain s3DiscoveryExtensionMain =
            new S3DiscoveryExtensionMain(mock(), mock());

    @BeforeEach
    void setUp(@TempDir final @NotNull File tempDir) {
        when(extensionStartInput.getExtensionInformation()).thenReturn(extensionInformation);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir);
    }

    @Test
    void test_start_success() {
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        assertThat(s3DiscoveryExtensionMain.s3DiscoveryCallback).isNotNull();
    }

    @Test
    void test_start_failed() {
        when(extensionInformation.getExtensionHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        assertThat(s3DiscoveryExtensionMain.s3DiscoveryCallback).isNull();
    }

    @Test
    void test_stop_success() {
        assertThatThrownBy(() -> {
            s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
            s3DiscoveryExtensionMain.extensionStop(extensionStopInput, extensionStopOutput);
        }).isInstanceOf(RuntimeException.class);
    }

    @Test
    void test_stop_no_start_failed() {
        when(extensionInformation.getExtensionHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        s3DiscoveryExtensionMain.extensionStop(extensionStopInput, extensionStopOutput);
        assertThat(s3DiscoveryExtensionMain.s3DiscoveryCallback).isNull();
    }
}
