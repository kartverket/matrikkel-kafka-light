FROM eclipse-temurin:23-jdk AS build
WORKDIR /workspace

# Laster inn minimalt av filer for å få caching av gradle inn.
COPY gradlew gradlew.bat settings.gradle.kts gradle.properties ./
COPY gradle ./gradle
RUN ./gradlew --version

COPY . .
RUN ./gradlew :apps:broker:installDist --no-daemon

FROM eclipse-temurin:23-jre
WORKDIR /app
COPY --from=build /workspace/apps/broker/build/install/broker /app
EXPOSE 8081
ENTRYPOINT ["/app/bin/broker"]