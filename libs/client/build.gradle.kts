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
            groupId = "no.kartverket.kafkalight"
            artifactId = "client"
            pom {
                name.set("kafka-light-client")
                description.set("Client for use with kafka light component")
                url.set("https://github.com/kartverket/matrikkel-kafka-light")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://github.com/kartverket/heimdall-common-utils/blob/main/LICENSE")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/kartverket/matrikkel-kafka-light.git")
                    developerConnection.set("scm:git:ssh://git@github.com/kartverket/matrikkel-kafka-light.git")
                    url.set("https://github.com/kartverket/matrikkel-kafka-light")
                }
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