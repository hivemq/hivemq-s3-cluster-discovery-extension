import java.net.URL

plugins {
    alias(libs.plugins.hivemq.extension)
    alias(libs.plugins.defaults)
    alias(libs.plugins.license)
    alias(libs.plugins.asciidoctor)
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
        from("README.adoc") { rename { "README.txt" } }
        from(tasks.asciidoctor)
    }
}

dependencies {
    hivemqProvided(libs.logback.classic)
    implementation(libs.owner)
    implementation(platform(libs.aws.sdkv2.bom))
    implementation(libs.aws.sdkv2.s3)
}

tasks.asciidoctor {
    sourceDirProperty.set(layout.projectDirectory)
    sources("README.adoc")
    secondarySources { exclude("**") }
}

/* ******************** test ******************** */

dependencies {
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

/* ******************** integration test ******************** */

dependencies {
    integrationTestCompileOnly(libs.jetbrains.annotations)
    //necessary as the localstack s3 service would not start without the old sdk
    integrationTestRuntimeOnly(libs.aws.sdkv1.s3)
    integrationTestImplementation(libs.okhttp)
    integrationTestImplementation(platform(libs.testcontainers.bom))
    integrationTestImplementation(libs.testcontainers)
    integrationTestImplementation(libs.testcontainers.junitJupiter)
    integrationTestImplementation(libs.testcontainers.localstack)
    integrationTestImplementation(libs.testcontainers.hivemq)
    integrationTestImplementation(libs.aws.sdkv2.s3)
}

tasks.integrationTest {
    dependsOn(unzipPrometheusExtension)
    classpath += project.objects.fileCollection().from(unzipPrometheusExtension.get().outputs.files.singleFile)
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

/* ******************** checks ******************** */

license {
    header = rootDir.resolve("HEADER")
    mapping("java", "SLASHSTAR_STYLE")
    exclude("**/template-s3discovery.properties")
    exclude("**/logback-test.xml")
}

/* ******************** run ******************** */

tasks.prepareHivemqHome {
    hivemqHomeDirectory.set(file("/path/to/a/hivemq/folder"))
}
