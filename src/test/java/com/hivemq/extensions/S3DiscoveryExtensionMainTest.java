package com.hivemq.extensions;

import com.hivemq.extension.sdk.api.parameter.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class S3DiscoveryExtensionMainTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public ExtensionStartInput extensionStartInput;
    @Mock
    public ExtensionStartOutput extensionStartOutput;
    @Mock
    public ExtensionStopInput extensionStopInput;
    @Mock
    public ExtensionStopOutput extensionStopOutput;
    @Mock
    public ExtensionInformation extensionInformation;

    private S3DiscoveryExtensionMain s3DiscoveryExtensionMain;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(extensionStartInput.getExtensionInformation()).thenReturn(extensionInformation);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(temporaryFolder.getRoot());
        s3DiscoveryExtensionMain = new S3DiscoveryExtensionMain();
    }

    @Test
    public void test_start_success() {
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        assertNotNull(s3DiscoveryExtensionMain.s3DiscoveryCallback);
    }

    @Test
    public void test_start_failed() {
        when(extensionInformation.getExtensionHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        assertNull(s3DiscoveryExtensionMain.s3DiscoveryCallback);
    }

    @Test(expected = RuntimeException.class)
    public void test_stop_success() {
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        s3DiscoveryExtensionMain.extensionStop(extensionStopInput, extensionStopOutput);
    }

    @Test
    public void test_stop_no_start_failed() {
        when(extensionInformation.getExtensionHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryExtensionMain.extensionStart(extensionStartInput, extensionStartOutput);
        s3DiscoveryExtensionMain.extensionStop(extensionStopInput, extensionStopOutput);
        assertNull(s3DiscoveryExtensionMain.s3DiscoveryCallback);
    }
}