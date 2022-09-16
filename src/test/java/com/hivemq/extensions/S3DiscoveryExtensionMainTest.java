package com.hivemq.extensions;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
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
        s3DiscoveryExtensionMain = new S3DiscoveryExtensionMain();
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
