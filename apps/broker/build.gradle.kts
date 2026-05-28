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
    implementation(libs.logback.classic)

    implementation(libs.flyway)
    implementation(libs.flyway.postgresql)
    implementation(libs.hikari)
    implementation(libs.postgresql)
    implementation(libs.h2)

    testImplementation(kotlin("test"))
    testImplementation(ktorLibs.server.testHost)
}
