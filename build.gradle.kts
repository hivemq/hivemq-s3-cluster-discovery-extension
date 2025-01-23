plugins {
    alias(libs.plugins.hivemq.extension)
    alias(libs.plugins.defaults)
    alias(libs.plugins.oci)
    alias(libs.plugins.license)
}

group = "com.hivemq.extensions"
description = "HiveMQ Extension for cluster discovery with S3 buckets"

hivemqExtension {
    name = "S3 Cluster Discovery Extension"
    author = "HiveMQ"
    priority = 1000
    startPriority = 10000
    sdkVersion = libs.versions.hivemq.extensionSdk

    resources {
        from("LICENSE")
    }
}

dependencies {
    compileOnly(libs.jetbrains.annotations)
    hivemqProvided(libs.logback.classic)
    implementation(libs.owner)
    implementation(libs.aws.sdkv2.s3)
}

oci {
    registries {
        dockerHub {
            optionalCredentials()
        }
    }
    imageMapping {
        mapModule("com.hivemq", "hivemq-enterprise") {
            toImage("hivemq/hivemq4")
        }
    }
    imageDefinitions {
        register("main") {
            allPlatforms {
                dependencies {
                    runtime("com.hivemq:hivemq-enterprise:latest") { isChanging = true }
                }
                layers {
                    layer("main") {
                        contents {
                            permissions("opt/hivemq/", 0b111_111_101)
                            permissions("opt/hivemq/extensions/", 0b111_111_101)
                            into("opt/hivemq/extensions") {
                                from(zipTree(tasks.hivemqExtensionZip.flatMap { it.archiveFile }))
                            }
                        }
                    }
                }
            }
        }
        register("integrationTest") {
            allPlatforms {
                dependencies {
                    runtime(project)
                    runtime("com.hivemq.extensions:hivemq-prometheus-extension")
                }
            }
        }
    }
}

@Suppress("UnstableApiUsage")
testing {
    suites {
        withType<JvmTestSuite> {
            useJUnitJupiter(libs.versions.junit.jupiter)
        }
        "test"(JvmTestSuite::class) {
            dependencies {
                compileOnly(libs.jetbrains.annotations)
                implementation(libs.mockito)
            }
        }
        "integrationTest"(JvmTestSuite::class) {
            dependencies {
                compileOnly(libs.jetbrains.annotations)
                implementation(libs.testcontainers)
                implementation(libs.testcontainers.junitJupiter)
                implementation(libs.testcontainers.hivemq)
                implementation(libs.testcontainers.localstack)
                implementation(libs.gradleOci.junitJupiter)
                implementation(libs.aws.sdkv2.s3)
                implementation(libs.okhttp)
                runtimeOnly(libs.logback.classic)
            }
            oci.of(this) {
                imageDependencies {
                    runtime(project) {
                        capabilities { requireFeature("integration-test") }
                    }.tag("latest")
                    runtime("localstack:localstack:3.3.0").tag("latest")
                }
            }
        }
    }
}

license {
    header = rootDir.resolve("HEADER")
    mapping("java", "SLASHSTAR_STYLE")
    exclude("**/template-s3discovery.properties")
    exclude("**/logback-test.xml")
}
