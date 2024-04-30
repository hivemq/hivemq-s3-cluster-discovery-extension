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

package com.hivemq.extensions.cluster.discovery.s3.config;

import com.hivemq.extension.sdk.api.parameter.ExtensionInformation;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;

import static com.hivemq.extensions.cluster.discovery.s3.ExtensionConstants.EXTENSION_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigurationReaderTest {

    private @NotNull ExtensionInformation extensionInformation;

    @BeforeEach
    void setUp(@TempDir final @NotNull File tempDir) {
        extensionInformation = mock(ExtensionInformation.class);
        when(extensionInformation.getExtensionHomeFolder()).thenReturn(tempDir);
    }

    @Test
    void test_readConfiguration_no_file() {
        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_successful() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_missing_bucket_name() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_missing_bucket_region() throws Exception {
        final String configuration = "s3-bucket-region:\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_invalid_bucket_region() throws Exception {
        final String configuration = "s3-bucket-region:us-east-123456\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_missing_credentials_type() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_invalid_credentials_type() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:default1234";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_credentials_type_access_key_successful() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_credentials_type_access_key_missing_key() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:\n" +
                "credentials-secret-access-key:secret-access-key";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_credentials_type_access_key_missing_secret() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:access_key\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_credentials_type_temporary_session_successful() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:temporary_session\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key\n" +
                "credentials-session-token:session-token";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_credentials_type_temporary_session_missing_session_token() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:180\n" +
                "credentials-type:temporary_session\n" +
                "credentials-access-key-id:access-key-id\n" +
                "credentials-secret-access-key:secret-access-key\n" +
                "credentials-session-token:";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_both_intervals_zero_successful() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:0\n" +
                "update-interval:0\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNotNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_both_intervals_same_value() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:180\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_update_interval_larger() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:150\n" +
                "update-interval:300\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_update_deactivated() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:180\n" +
                "update-interval:0\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_expiration_deactivated() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:0\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_missing_expiration() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:\n" +
                "update-interval:180\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }

    @Test
    void test_readConfiguration_missing_update() throws Exception {
        final String configuration = "s3-bucket-region:us-east-1\n" +
                "s3-bucket-name:hivemq\n" +
                "file-prefix:hivemq/cluster/nodes/\n" +
                "file-expiration:360\n" +
                "update-interval:\n" +
                "credentials-type:default";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final ConfigurationReader configurationReader = new ConfigurationReader(extensionInformation);
        assertNull(configurationReader.readConfiguration());
    }
}
