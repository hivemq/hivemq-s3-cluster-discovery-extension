package com.hivemq.plugin;

import com.hivemq.plugin.api.parameter.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

public class S3DiscoveryPluginMainTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Mock
    public PluginStartInput pluginStartInput;
    @Mock
    public PluginStartOutput pluginStartOutput;
    @Mock
    public PluginStopInput pluginStopInput;
    @Mock
    public PluginStopOutput pluginStopOutput;
    @Mock
    public PluginInformation pluginInformation;

    private S3DiscoveryPluginMain s3DiscoveryPluginMain;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(pluginStartInput.getPluginInformation()).thenReturn(pluginInformation);
        when(pluginInformation.getPluginHomeFolder()).thenReturn(temporaryFolder.getRoot());
        s3DiscoveryPluginMain = new S3DiscoveryPluginMain();
    }

    @Test
    public void test_start_success() {
        s3DiscoveryPluginMain.pluginStart(pluginStartInput, pluginStartOutput);
        Assert.assertNotNull(s3DiscoveryPluginMain.s3DiscoveryCallback);
    }

    @Test
    public void test_start_failed() {
        when(pluginInformation.getPluginHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryPluginMain.pluginStart(pluginStartInput, pluginStartOutput);
        Assert.assertNull(s3DiscoveryPluginMain.s3DiscoveryCallback);
    }

    @Test(expected = RuntimeException.class)
    public void test_stop_success() {
        s3DiscoveryPluginMain.pluginStart(pluginStartInput, pluginStartOutput);
        s3DiscoveryPluginMain.pluginStop(pluginStopInput, pluginStopOutput);
    }

    @Test
    public void test_stop_no_start_failed() {
        when(pluginInformation.getPluginHomeFolder()).thenThrow(new NullPointerException());
        s3DiscoveryPluginMain.pluginStart(pluginStartInput, pluginStartOutput);
        s3DiscoveryPluginMain.pluginStop(pluginStopInput, pluginStopOutput);
        Assert.assertNull(s3DiscoveryPluginMain.s3DiscoveryCallback);
    }
}