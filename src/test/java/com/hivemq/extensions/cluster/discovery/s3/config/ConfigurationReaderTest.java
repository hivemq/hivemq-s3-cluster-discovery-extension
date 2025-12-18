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
import static org.assertj.core.api.Assertions.assertThat;
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
        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_successful() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNotNull();
    }

    @Test
    void test_readConfiguration_missing_bucket_name() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_missing_bucket_region() throws Exception {
        final var configuration = """
                s3-bucket-region:
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_invalid_bucket_region() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-123456
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_missing_credentials_type() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_invalid_credentials_type() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:default1234""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_credentials_type_access_key_successful() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNotNull();
    }

    @Test
    void test_readConfiguration_credentials_type_access_key_missing_key() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:
                credentials-secret-access-key:secret-access-key""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_credentials_type_access_key_missing_secret() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:access_key
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_credentials_type_temporary_session_successful() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:temporary_session
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key
                credentials-session-token:session-token""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNotNull();
    }

    @Test
    void test_readConfiguration_credentials_type_temporary_session_missing_session_token() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:180
                credentials-type:temporary_session
                credentials-access-key-id:access-key-id
                credentials-secret-access-key:secret-access-key
                credentials-session-token:""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_both_intervals_zero_successful() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:0
                update-interval:0
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNotNull();
    }

    @Test
    void test_readConfiguration_both_intervals_same_value() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:180
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_update_interval_larger() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:150
                update-interval:300
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_update_deactivated() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:180
                update-interval:0
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_expiration_deactivated() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:0
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_missing_expiration() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:
                update-interval:180
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }

    @Test
    void test_readConfiguration_missing_update() throws Exception {
        final var configuration = """
                s3-bucket-region:us-east-1
                s3-bucket-name:hivemq
                file-prefix:hivemq/cluster/nodes/
                file-expiration:360
                update-interval:
                credentials-type:default""";
        Files.writeString(extensionInformation.getExtensionHomeFolder().toPath().resolve(EXTENSION_CONFIGURATION),
                configuration);

        final var configurationReader = new ConfigurationReader(extensionInformation);
        assertThat(configurationReader.readConfiguration()).isNull();
    }
}
