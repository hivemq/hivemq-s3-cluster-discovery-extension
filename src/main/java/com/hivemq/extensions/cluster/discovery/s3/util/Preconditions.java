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

package com.hivemq.extensions.cluster.discovery.s3.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
