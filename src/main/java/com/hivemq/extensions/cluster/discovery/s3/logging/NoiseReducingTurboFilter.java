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

package com.hivemq.extensions.cluster.discovery.s3.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Marker;

import java.util.Set;

class NoiseReducingTurboFilter extends TurboFilter {

    private static final @NotNull Set<String> NOISY_LOGGER_NAMES = Set.of("software.amazon.awssdk.request",
            "software.amazon.awssdk.requestId",
            "software.amazon.awssdk.auth.signer.Aws4Signer",
            "software.amazon.awssdk.core.interceptor.ExecutionInterceptorChain",
            "software.amazon.awssdk.core.internal.io.SdkLengthAwareInputStream",
            "software.amazon.awssdk.http.apache.internal.conn.SdkTlsSocketFactory");

    @Override
    public @NotNull FilterReply decide(
            final @Nullable Marker marker,
            final @Nullable Logger logger,
            final @Nullable Level level,
            final @Nullable String format,
            final @Nullable Object @NotNull [] params,
            final @Nullable Throwable t) {
        if (level != null && level.levelInt > Level.TRACE_INT && logger != null && isNoisy(logger.getName())) {
            logger.trace(marker, format, params, t);
            return FilterReply.DENY;
        }
        return FilterReply.NEUTRAL;
    }

    private static boolean isNoisy(final @Nullable String loggerName) {
        return loggerName != null && (NOISY_LOGGER_NAMES.contains(loggerName));
    }
}
