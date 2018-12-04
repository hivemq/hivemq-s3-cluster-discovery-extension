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

package com.hivemq.plugin.config;

import com.hivemq.plugin.api.annotations.NotNull;

/**
 * @author Florian Limpöck
 * @author Abdullah Imal
 * @since 4.0.0
 */
public enum AuthenticationType {

    DEFAULT("default"),
    ENVIRONMENT_VARIABLES("environment_variables"),
    JAVA_SYSTEM_PROPERTIES("java_system_properties"),
    USER_CREDENTIALS_FILE("user_credentials_file"),
    INSTANCE_PROFILE_CREDENTIALS("instance_profile_credentials"),
    ACCESS_KEY("access_key"),
    TEMPORARY_SESSION("temporary_session");

    private final String name;

    AuthenticationType(@NotNull final String name) {
        this.name = name;
    }

    @NotNull
    public static AuthenticationType fromName(@NotNull final String name) throws IllegalArgumentException {
        for (final AuthenticationType type : values()) {
            if (name.contentEquals(type.getName())) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown credentials type " + name);
    }

    @NotNull
    public String getName() {
        return name;
    }
}
