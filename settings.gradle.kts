rootProject.name = "hivemq-s3-cluster-discovery-extension"

if (file("../hivemq-prometheus-extension").exists()) {
    includeBuild("../hivemq-prometheus-extension")
}
