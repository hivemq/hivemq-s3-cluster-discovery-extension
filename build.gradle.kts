import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

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

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.compileJava {
    javaCompiler = javaToolchains.compilerFor {
        languageVersion = JavaLanguageVersion.of(11)
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
                layer("main") {
                    contents {
                        permissions("opt/hivemq/", 0b111_111_101)
                        permissions("opt/hivemq/extensions/", 0b111_111_101)
                        into("opt/hivemq/extensions") {
                            permissions("*/", 0b111_111_101)
                            permissions("*/hivemq-extension.xml", 0b110_110_100)
                            permissions("*/conf/", 0b111_111_101)
                            permissions("*/conf/config.properties", 0b110_110_100)
                            from(zipTree(tasks.hivemqExtensionZip.flatMap { it.archiveFile }))
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
                implementation(libs.assertj)
                implementation(libs.mockito)
                implementation(libs.logback.classic)
            }
            targets.configureEach {
                testTask {
                    testLogging {
                        events = setOf(
                            TestLogEvent.STARTED,
                            TestLogEvent.PASSED,
                            TestLogEvent.SKIPPED,
                            TestLogEvent.FAILED,
                            TestLogEvent.STANDARD_ERROR,
                        )
                        exceptionFormat = TestExceptionFormat.FULL
                        showStandardStreams = true
                    }
                    reports {
                        junitXml.isOutputPerTestCase = true
                    }
                }
            }
        }
        "integrationTest"(JvmTestSuite::class) {
            dependencies {
                compileOnly(libs.jetbrains.annotations)
                implementation(libs.assertj)
                implementation(libs.testcontainers)
                implementation(libs.testcontainers.junitJupiter)
                implementation(libs.testcontainers.hivemq)
                implementation(libs.testcontainers.localstack)
                implementation(libs.gradleOci.junitJupiter)
                implementation(libs.aws.sdkv2.s3)
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
    exclude("**/template-config.properties")
    exclude("**/logback-test.xml")
}

// configure reproducible builds
tasks.withType<AbstractArchiveTask>().configureEach {
    // normalize file permissions for reproducibility
    // files: 0644 (rw-r--r--), directories: 0755 (rwxr-xr-x)
    filePermissions {
        unix("0644")
    }
    dirPermissions {
        unix("0755")
    }
}

tasks.withType<JavaCompile>().configureEach {
    // ensure consistent compilation across different JDK versions
    options.compilerArgs.addAll(listOf(
        // include parameter names for reflection (improves consistency)
        "-parameters"
    ))
}
