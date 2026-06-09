plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    implementation(ktorLibs.client.core)
    implementation(ktorLibs.client.contentNegotiation)
    implementation(ktorLibs.serialization.kotlinx.json)

    testImplementation(ktorLibs.client.mock)
    testImplementation(kotlin("test"))
    testImplementation(libs.bundles.testEcosystem)
}