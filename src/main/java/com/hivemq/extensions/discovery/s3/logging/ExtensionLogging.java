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

package com.hivemq.extensions.discovery.s3.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import ch.qos.logback.classic.spi.TurboFilterList;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class ExtensionLogging {

    private final @NotNull NoiseReducingTurboFilter noiseReducingTurboFilter;

    public ExtensionLogging() {
        noiseReducingTurboFilter = new NoiseReducingTurboFilter();
    }

    private final @NotNull AtomicBoolean stopped = new AtomicBoolean(false);

    public void start() {
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.addListener(new LogbackChangeListener());
        context.addTurboFilter(noiseReducingTurboFilter);
    }

    public void stop() {
        stopped.set(true);
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        final TurboFilterList turboFilterList = context.getTurboFilterList();
        turboFilterList.remove(noiseReducingTurboFilter);
    }

    private class LogbackChangeListener implements LoggerContextListener {

        @Override
        public boolean isResetResistant() {
            return true;
        }

        @Override
        public void onStart(final @NotNull LoggerContext context) {
            // noop
        }

        @Override
        public void onReset(final @NotNull LoggerContext context) {
            if (!stopped.get()) {
                context.addTurboFilter(noiseReducingTurboFilter);
            }
        }

        @Override
        public void onStop(final @NotNull LoggerContext context) {
            // noop
        }

        @Override
        public void onLevelChange(final @NotNull Logger logger, final @NotNull Level level) {
            // noop
        }
    }
}