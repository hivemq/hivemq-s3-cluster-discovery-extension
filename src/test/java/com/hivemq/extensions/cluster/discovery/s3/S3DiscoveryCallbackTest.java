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

import com.codahale.metrics.Counter;
import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryInput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterDiscoveryOutput;
import com.hivemq.extension.sdk.api.services.cluster.parameter.ClusterNodeAddress;
import com.hivemq.extensions.cluster.discovery.s3.aws.HiveMQS3Client;
import com.hivemq.extensions.cluster.discovery.s3.aws.S3BucketResponse;
import com.hivemq.extensions.cluster.discovery.s3.config.ConfigurationReader;
import com.hivemq.extensions.cluster.discovery.s3.config.S3Config;
import com.hivemq.extensions.cluster.discovery.s3.util.ClusterNodeFileUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_CONFIGURATION;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3DiscoveryCallbackTest {

    private @NotNull ExtensionInformation extensionInformation;
    private @NotNull ClusterDiscoveryInput clusterDiscoveryInput;
    private @NotNull ClusterDiscoveryOutput clusterDiscoveryOutput;
    private @NotNull HiveMQS3Client hiveMQS3Client;
    private @NotNull S3DiscoveryCallback s3DiscoveryCallback;
    private @NotNull ConfigurationReader configurationReader;
    private @NotNull S3DiscoveryMetrics s3DiscoveryMetrics;

    @BeforeEach
    void setUp(@TempDir final @NotNull File tempDir) throws Exception {
        extensionInformation = mock(ExtensionInformation.class);
        clusterDiscoveryInput = mock(ClusterDiscoveryInput.class);
        clusterDiscoveryOutput = mock(ClusterDiscoveryOutput.class);
        s3DiscoveryMetrics = mock(S3DiscoveryMetrics.class);
        when(clusterDiscoveryInput.getOwnClusterId()).thenReturn("ABCD12");
        when(clusterDiscoveryInput.getOwnAddress()).thenReturn(new ClusterNodeAddress("127.0.0.1", 7800));
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir);
        when(s3DiscoveryMetrics.getQuerySuccessCount()).thenReturn(mock(Counter.class));
        when(s3DiscoveryMetrics.getQueryFailedCount()).thenReturn(mock(Counter.class));

        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        configurationReader = new ConfigurationReader(extensionInformation);
        hiveMQS3Client = mock(HiveMQS3Client.class);
        final S3Config s3Config = configurationReader.readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);

        s3DiscoveryCallback = new S3DiscoveryCallback(hiveMQS3Client, s3DiscoveryMetrics);
        when(hiveMQS3Client.checkBucket()).thenReturn(new S3BucketResponse("hivemq123456", 200, null));
    }

    @Test
    void test_init_success() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        when(hiveMQS3Client.getObject(any())).then(ignored -> createS3Object());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_provide_current_nodes_exception_getting_node_files() {
        doThrow(S3Exception.class).when(hiveMQS3Client).getObjects();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_amazons3exception_getting_node_file() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        doThrow(S3Exception.class).when(hiveMQS3Client).getObject(any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_exception_getting_node_file() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        doThrow(S3Exception.class).when(hiveMQS3Client).getObject(any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_s3objectsummary_null() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectNullList());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_expired_files() throws Exception {
        final String configuration = "s3-bucket-region:us-east-2\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:2\n" +
                "update-interval:1\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        //Object needs to be created prior the sleep as it adds the current timestamp to the ClusterNodeFile.
        //Therefore, then(...) cannot be used as it would create the object on execution.
        final String object = createS3Object();
        when(hiveMQS3Client.getObject(any())).thenReturn(object);

        // Wait for files to expire
        TimeUnit.SECONDS.sleep(2);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_truncated() {
        when(hiveMQS3Client.getObject(any())).then(ignored -> createS3Object());
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectListTruncated());
        when(hiveMQS3Client.getNextBatchOfObjects(any())).then(ignored -> extendedObjectListNotTruncated());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_provide_current_nodes_s3object_null() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        when(hiveMQS3Client.getObject(any())).thenReturn(null);

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_s3object_content_blank() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        when(hiveMQS3Client.getObject(any())).thenReturn(createS3ObjectBlankContent());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_s3object_content_null() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        when(hiveMQS3Client.getObject(any())).thenReturn(createS3ObjectNullContent());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_provide_current_nodes_parse_failed() {
        when(hiveMQS3Client.getObjects()).then(ignored -> extendedObjectList());
        when(hiveMQS3Client.getObject(any())).thenReturn(createS3ObjectInvalid());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput).provideCurrentNodes(new ArrayList<>());
    }

    @Test
    void test_init_save_own_file_failed() {
        doThrow(S3Exception.class).when(hiveMQS3Client).saveObject(any(), any());

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_bucket_does_not_exist() {
        when(hiveMQS3Client.checkBucket()).thenReturn(new S3BucketResponse("hivemq123456",
                404,
                NoSuchBucketException.builder().build()));

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_create_failed_config() {
        doThrow(new IllegalStateException("Config is not valid.")).when(hiveMQS3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client, never()).checkBucket();
        verify(hiveMQS3Client).createOrUpdate();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_create_failed_by_amazons3() {
        doThrow(S3Exception.builder().message("AmazonS3 couldn't be build.").build()).when(hiveMQS3Client)
                .createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client, never()).checkBucket();
        verify(hiveMQS3Client).createOrUpdate();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_bucket_check_failed() {
        when(hiveMQS3Client.checkBucket()).thenReturn(new S3BucketResponse("hivemq123456", 0, null));

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client).createOrUpdate();
        verify(hiveMQS3Client).checkBucket();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_config_invalid() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq1234\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);
        doThrow(IllegalStateException.class).when(hiveMQS3Client).createOrUpdate();

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(hiveMQS3Client, never()).checkBucket();
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_init_no_config() throws IOException {
        Files.delete(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION));

        s3DiscoveryCallback = new S3DiscoveryCallback(configurationReader, s3DiscoveryMetrics);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_success_same_config() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_success_new_config() throws Exception {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        final String configuration = "s3-bucket-region:us-east-2\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:120\n" +
                "update-interval:60\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_new_config_no_bucket_no_existing_client() throws Exception {
        Files.delete(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION));

        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        final String configuration = "s3-bucket-region:us-east-2\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:120\n" +
                "update-interval:60\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);
        when(hiveMQS3Client.checkBucket()).thenReturn(new S3BucketResponse("hivemq123456",
                404,
                NoSuchBucketException.builder().build()));

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        verify(clusterDiscoveryOutput, times(1)).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_config_missing_init_success() throws IOException {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        Files.delete(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION));

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_config_still_missing() throws IOException {
        Files.delete(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION));
        when(hiveMQS3Client.getS3Config()).thenReturn(null);
        doThrow(IllegalStateException.class).when(hiveMQS3Client).createOrUpdate();

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        verify(clusterDiscoveryOutput, never()).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_file_expired() throws Exception {
        final String configuration = "s3-bucket-region:us-east-2\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:5\n" +
                "update-interval:1\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        // Wait for file to expire
        TimeUnit.MILLISECONDS.sleep(1500);

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        verify(hiveMQS3Client, times(2)).saveObject(any(), any());
        verify(clusterDiscoveryOutput, times(2)).provideCurrentNodes(anyList());
    }

    @Test
    void test_reload_file_exception() throws Exception {
        final String configuration = "s3-bucket-region:us-east-2\n" +
                "s3-bucket-name:hivemq123456\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:5\n" +
                "update-interval:1\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final S3Config s3Config = new ConfigurationReader(extensionInformation).readConfiguration();
        when(hiveMQS3Client.getS3Config()).thenReturn(s3Config);
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);

        // Wait for file to expire
        TimeUnit.SECONDS.sleep(1);
        doThrow(S3Exception.class).when(hiveMQS3Client).saveObject(any(), any());

        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        verify(hiveMQS3Client, times(2)).saveObject(any(), any());
        verify(clusterDiscoveryOutput, times(1)).provideCurrentNodes(anyList());
    }

    @Test
    void test_destroy_success() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.destroy(clusterDiscoveryInput);

        verify(hiveMQS3Client, times(1)).deleteObject(any());
    }

    @Test
    void test_destroy_no_own_file() {
        s3DiscoveryCallback.destroy(clusterDiscoveryInput);
        verify(hiveMQS3Client, never()).deleteObject(any());
    }

    @Test
    void test_destroy_delete_own_file_failed() {
        s3DiscoveryCallback.init(clusterDiscoveryInput, clusterDiscoveryOutput);
        s3DiscoveryCallback.reload(clusterDiscoveryInput, clusterDiscoveryOutput);

        when(hiveMQS3Client.getObject(any())).then(ignored -> createS3Object());

        doThrow(S3Exception.class).when(hiveMQS3Client).deleteObject(any());
        s3DiscoveryCallback.destroy(clusterDiscoveryInput);

        verify(hiveMQS3Client, times(1)).deleteObject(any());
    }

    private @NotNull String createS3Object() {
        final ClusterNodeFile clusterNodeFile =
                new ClusterNodeFile("ABCD12", new ClusterNodeAddress("127.0.0.1", 1883));
        return clusterNodeFile.toString();
    }

    private @NotNull String createS3ObjectInvalid() {
        return ClusterNodeFileUtil.createClusterNodeFileString("3", "3", "3", "3", "3");
    }

    private @NotNull String createS3ObjectBlankContent() {
        return "  ";
    }

    private @Nullable String createS3ObjectNullContent() {
        return null;
    }

    private @NotNull ListObjectsV2Response extendedObjectList() {
        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        final S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn("ABCD12");
        final List<S3Object> objects = new ArrayList<>();
        objects.add(s3Object);
        when(listObjectsV2Response.contents()).thenReturn(objects);
        return listObjectsV2Response;
    }

    private @NotNull ListObjectsV2Response extendedObjectNullList() {
        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        final List<S3Object> objects = new ArrayList<>();
        objects.add(null);
        objects.add(null);
        when(listObjectsV2Response.contents()).thenReturn(objects);
        return listObjectsV2Response;
    }

    private @NotNull ListObjectsV2Response extendedObjectListTruncated() {
        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        final S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn("ABCD12");
        final List<S3Object> objects = new ArrayList<>();
        objects.add(s3Object);
        when(listObjectsV2Response.contents()).thenReturn(objects);
        when(listObjectsV2Response.isTruncated()).thenReturn(true);
        return listObjectsV2Response;
    }

    private @NotNull ListObjectsV2Response extendedObjectListNotTruncated() {
        final ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        final S3Object s3Object = mock(S3Object.class);
        when(s3Object.key()).thenReturn("ABCD12");
        final List<S3Object> objects = new ArrayList<>();
        objects.add(s3Object);
        when(listObjectsV2Response.contents()).thenReturn(objects);
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        return listObjectsV2Response;
    }
}
