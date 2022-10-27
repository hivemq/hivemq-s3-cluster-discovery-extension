package com.hivemq.extensions.discovery.s3.aws;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;

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


