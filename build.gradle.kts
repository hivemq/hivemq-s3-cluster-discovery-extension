import java.net.URI

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
                    runtime("hivemq:hivemq4:4.9.0").tag("latest")
                    runtime("localstack:localstack:3.3.0").tag("latest")
                }
            }
        }
    }
}

val downloadPrometheusExtension by tasks.registering {
    val prometheusExtension =
        "https://github.com/hivemq/hivemq-prometheus-extension/releases/download/4.0.6/hivemq-prometheus-extension-4.0.6.zip"
    val zipFile = File(temporaryDir, "hivemq-prometheus-extension.zip")
    doLast {
        URI(prometheusExtension).toURL().openStream().use { input ->
            zipFile.outputStream().use { output -> input.copyTo(output) }
        }
    }
    outputs.file(zipFile)
}

val unzipPrometheusExtension by tasks.registering(Sync::class) {
    from(zipTree(downloadPrometheusExtension.map { it.outputs.files.singleFile }))
    into({ temporaryDir })
}

tasks.integrationTest {
    classpath += unzipPrometheusExtension.get().outputs.files
}

license {
    header = rootDir.resolve("HEADER")
    mapping("java", "SLASHSTAR_STYLE")
    exclude("**/template-s3discovery.properties")
    exclude("**/logback-test.xml")
}
