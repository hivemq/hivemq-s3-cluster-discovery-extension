package com.hivemq.extensions.util;

import com.hivemq.extension.sdk.api.annotations.Nullable;

/**
 * @author Abdullah Imal
 */
public final class StringUtil {

    private StringUtil() {
    }

    public static boolean isNullOrBlank(final @Nullable String value) {
        return value == null || value.isBlank();
    }
}
