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

package com.hivemq.extensions.cluster.discovery.s3.aws;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

public class S3BucketResponse {

    private final @NotNull String bucketName;
    private final @NotNull Status status;
    private final @Nullable Throwable throwable;

    public S3BucketResponse(
            final @NotNull String bucketName, final int statusCode, final @Nullable Throwable throwable) {
        this.bucketName = bucketName;
        switch (statusCode) {
            case 200:
                this.status = Status.EXISTING;
                break;
            case 404:
                this.status = Status.NOT_EXISTING;
                break;
            case 403:
                this.status = Status.NO_PERMISSION;
                break;
            default:
                this.status = Status.OTHER;
                break;
        }
        this.throwable = throwable;
    }

    public @NotNull String getBucketName() {
        return bucketName;
    }

    public @NotNull Status getStatus() {
        return status;
    }

    public boolean isSuccessful() {
        return status.isSuccessful();
    }

    public @NotNull Optional<Throwable> getThrowable() {
        return Optional.ofNullable(throwable);
    }

    public enum Status {
        EXISTING(true),
        NOT_EXISTING(false),
        NO_PERMISSION(false),
        OTHER(false);

        private final boolean successful;

        Status(final boolean successful) {
            this.successful = successful;
        }

        public boolean isSuccessful() {
            return successful;
        }
    }
}


