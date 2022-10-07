import java.net.URL

plugins {
    id("com.hivemq.extension")
    id("com.github.hierynomus.license")
    id("com.github.sgtsilvio.gradle.utf8")
    id("org.asciidoctor.jvm.convert")
}

group = "com.hivemq.extensions"
description = "HiveMQ Extension for cluster discovery with S3 buckets"

hivemqExtension {
    name.set("S3 Cluster Discovery Extension")
    author.set("HiveMQ")
    priority.set(1000)
    startPriority.set(10000)
    sdkVersion.set("${property("hivemq-extension-sdk.version")}")
}

dependencies {
    hivemqProvided("ch.qos.logback:logback-classic:${property("logback.version")}")
    implementation("org.aeonbits.owner:owner:${property("owner.version")}")
    implementation(platform("software.amazon.awssdk:bom:${property("aws-bom.version")}"))
    implementation("software.amazon.awssdk:s3")
}

/* ******************** resources ******************** */

val prepareAsciidoc by tasks.registering(Sync::class) {
    from("README.adoc").into({ temporaryDir })
}

tasks.asciidoctor {
    dependsOn(prepareAsciidoc)
    sourceDir(prepareAsciidoc.map { it.destinationDir })
}

hivemqExtension.resources {
    from("LICENSE")
    from("README.adoc") { rename { "README.txt" } }
    from(tasks.asciidoctor)
}

/* ******************** test ******************** */

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:${property("junit-jupiter.version")}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.mockito:mockito-core:${property("mockito.version")}")
    testImplementation("org.mockito:mockito-junit-jupiter:${property("mockito.version")}")
    //testRuntimeOnly("ch.qos.logback:logback-classic:${property("logback.version")}")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

/* ******************** integration test ******************** */

dependencies {
    //necessary as the localstack s3 service would not start without the old sdk
    integrationTestImplementation("com.amazonaws:aws-java-sdk-s3:${property("aws-legacy-sdk.version")}")
    integrationTestImplementation("com.squareup.okhttp3:okhttp:${property("ok-http.version")}")
    integrationTestImplementation(platform("org.testcontainers:testcontainers-bom:${property("testcontainers.version")}"))
    integrationTestImplementation("org.testcontainers:testcontainers")
    integrationTestImplementation("org.testcontainers:junit-jupiter")
    integrationTestImplementation("org.testcontainers:localstack")
    integrationTestImplementation("org.testcontainers:hivemq")
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
}
