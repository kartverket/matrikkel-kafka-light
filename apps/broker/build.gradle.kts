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

    testImplementation(kotlin("test"))
    testImplementation(ktorLibs.server.testHost)
}
