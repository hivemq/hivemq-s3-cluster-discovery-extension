import java.net.URL

plugins {
    alias(libs.plugins.hivemq.extension)
    alias(libs.plugins.defaults)
    alias(libs.plugins.license)
}

group = "com.hivemq.extensions"
description = "HiveMQ Extension for cluster discovery with S3 buckets"

hivemqExtension {
    name.set("S3 Cluster Discovery Extension")
    author.set("HiveMQ")
    priority.set(1000)
    startPriority.set(10000)
    sdkVersion.set(libs.versions.hivemq.extensionSdk)

    resources {
        from("LICENSE")
    }
}

dependencies {
    hivemqProvided(libs.logback.classic)
    implementation(libs.owner)
    implementation(libs.aws.sdkv2.s3)
}

@Suppress("UnstableApiUsage")
testing {
    suites {
        withType<JvmTestSuite> {
            useJUnitJupiter(libs.versions.junit.jupiter)
        }
        "test"(JvmTestSuite::class) {
            dependencies {
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
                //necessary as the localstack s3 service would not start without the old sdk
                runtimeOnly(libs.aws.sdkv1.s3)
                implementation(libs.aws.sdkv2.s3)
                implementation(libs.okhttp)
                runtimeOnly(libs.logback.classic)
            }
        }
    }
}

val downloadPrometheusExtension by tasks.registering {
    val prometheusExtension =
        "https://github.com/hivemq/hivemq-prometheus-extension/releases/download/4.0.6/hivemq-prometheus-extension-4.0.6.zip"
    val zipFile = File(temporaryDir, "hivemq-prometheus-extension.zip")
    doLast {
        URL(prometheusExtension).openStream().use { input ->
            zipFile.outputStream().use { output -> input.copyTo(output) }
        }
    }
    outputs.file(zipFile)
}

val unzipPrometheusExtension by tasks.registering(Sync::class) {
    from(downloadPrometheusExtension.map { zipTree(it.outputs.files.singleFile) })
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
