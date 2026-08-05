rootProject.name = "matrikkel-kafka-light"

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/kartverket/matrikkel")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: System.getenv("GITHUB_USER") ?: "token"
                password = System.getenv("PACKAGES_TOKEN") ?: System.getenv("KV_PACKAGES_PAT") ?: System.getenv("GH_PACKAGES_PAT")
            }
        }
    }
    versionCatalogs {
        create("ktorLibs").from("io.ktor:ktor-version-catalog:3.5.2")
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

include(":apps:broker")
include(":libs:client")