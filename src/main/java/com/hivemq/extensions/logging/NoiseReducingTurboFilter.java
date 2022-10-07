package com.hivemq.extensions.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import org.slf4j.Marker;

import java.util.Set;

public class NoiseReducingTurboFilter extends TurboFilter {
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
