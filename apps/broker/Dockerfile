FROM eclipse-temurin:23-jdk AS build
WORKDIR /workspace

# Laster inn minimalt av filer for å få caching av gradle inn.
COPY ../../gradlew gradlew.bat settings.gradle.kts gradle.properties ./
COPY ../../gradle ./gradle
RUN ./gradlew --version

COPY ../.. .
RUN ./gradlew :installDist --no-daemon

FROM eclipse-temurin:23-jre
USER app
WORKDIR /app
COPY --from=build /workspace/build/install/matrikkel-kafka-light /app
EXPOSE 8080
ENTRYPOINT ["bin/matrikkel-kafka-light"]