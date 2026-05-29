FROM eclipse-temurin:23-jdk-alpine@sha256:e595e82dfab54cf44d8a7e7803d9d3f37842ce370d3156d6a5b30317d2774910 AS build
WORKDIR /workspace

# Laster inn minimalt av filer for å få caching av gradle inn.
COPY gradlew gradlew.bat settings.gradle.kts gradle.properties ./
COPY gradle ./gradle
RUN ./gradlew --version

COPY . .
RUN ./gradlew :apps:broker:installDist --no-daemon

FROM eclipse-temurin:23-jre-alpine@sha256:88593498863c64b43be16e8357a3c70ea475fc20a93bf1e07f4609213a357c87
WORKDIR /app
COPY --from=build /workspace/apps/broker/build/install/broker /app
EXPOSE 8081
ENTRYPOINT ["/app/bin/broker"]