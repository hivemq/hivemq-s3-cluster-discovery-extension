package com.hivemq.extensions.util;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;

/**
 * @author Abdullah Imal
 */
public final class Preconditions {

    private Preconditions() {
    }

    public static void checkArgument(final boolean condition, final @NotNull String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void checkNotNull(final @Nullable Object object, final @NotNull String name) {
        if (object == null) {
            throw new NullPointerException(String.format("'%s' must not be null!", name));
        }
    }

    public static void checkNotNullOrBlank(final @Nullable String value, final @NotNull String name) {
        checkNotNull(value, name);
        if (StringUtil.isNullOrBlank(value)) {
            throw new IllegalArgumentException(String.format("'%s' must not be null or blank!", name));
        }
    }
}
