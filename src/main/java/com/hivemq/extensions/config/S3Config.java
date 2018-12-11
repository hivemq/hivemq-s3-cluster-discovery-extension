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

package com.hivemq.extensions.config;

import com.amazonaws.services.s3.internal.Constants;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import org.aeonbits.owner.Config;

/**
 * @author Abdullah Imal
 * @since 4.0.0
 */
public interface S3Config extends Config {

    @Key("s3-bucket-name")
    @NotNull
    String getBucketName();

    @Key("s3-bucket-region")
    @NotNull
    String getBucketRegionName();

    @Key("file-prefix")
    @NotNull
    @DefaultValue("")
    String getFilePrefix();

    @Key("file-expiration")
    @NotNull
    Long getFileExpirationInSeconds();

    @Key("update-interval")
    @NotNull
    Long getFileUpdateIntervalInSeconds();

    @Key("s3-endpoint")
    @NotNull
    @DefaultValue(Constants.S3_HOSTNAME)
    String getEndpoint();

    @Key("s3-endpoint-region")
    @Nullable
    String getEndpointRegionName();

    @Key("s3-path-style-access")
    @Nullable
    Boolean getPathStyleAccess();

    @Key("credentials-type")
    @NotNull
    String getAuthenticationTypeName();

    @Key("credentials-access-key-id")
    @Nullable
    String getAccessKeyId();

    @Key("credentials-secret-access-key")
    @Nullable
    String getAccessKeySecret();

    @Key("credentials-session-token")
    @Nullable
    String getSessionToken();
}
