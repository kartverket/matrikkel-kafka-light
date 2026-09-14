plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("maven-publish")
}

dependencies {
    implementation(ktorLibs.client.core)
    implementation(ktorLibs.client.cio)
    implementation(ktorLibs.client.contentNegotiation)
    implementation(ktorLibs.serialization.kotlinx.cbor)
    implementation(ktorLibs.serialization.kotlinx.json)

    testImplementation(ktorLibs.client.mock)
    testImplementation(kotlin("test"))
    testImplementation(libs.bundles.testEcosystem)
    testImplementation(libs.bundles.testEcosystem)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = "no.kartverket.matrikkel"
            artifactId = "matrikkel-kafka-light"
            version = providers.systemProperty("suffix").orElse("0-SNAPSHOT").get()
            pom {
                description = "Implementation of no.kartverket.matrikkel:matrikkel-kafka-light:${version}"
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/kartverket/matrikkel-kafka-light")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}