plugins {
    id("buildsrc.convention.kotlin-jvm")
    application
}

application {
    mainClass = "no.kartverket.matrikkel.broker.MainKt"
}

dependencies {
    implementation(ktorLibs.server.config.yaml)
    implementation(ktorLibs.server.core)
    implementation(ktorLibs.server.netty)
    implementation(ktorLibs.server.auth)
    implementation(ktorLibs.server.auth.jwt)
    implementation(ktorLibs.server.metrics.micrometer)
    implementation(ktorLibs.client.cio)
    implementation(ktorLibs.client.contentNegotiation)
    implementation(ktorLibs.serialization.kotlinx.json)
    implementation(libs.slf4j)
    implementation(libs.logback.classic)
    implementation(libs.micrometerPrometheus)

    implementation(libs.flyway)
    implementation(libs.flyway.postgresql)
    implementation(libs.hikari)
    implementation(libs.postgresql)
    implementation(libs.h2)
    implementation(libs.kotliquery)

    testImplementation(kotlin("test"))
    testImplementation(ktorLibs.server.testHost)
    testImplementation(libs.bundles.testEcosystem)
}
