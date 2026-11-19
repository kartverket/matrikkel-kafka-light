FROM dhi.io/eclipse-temurin:25.0.5.8-alpine3.24@sha256:f3263435e20c3521197b318a3cd06f7fd6be2380d454e52c964576b1b38047c3 AS runtime

COPY apps/broker/build/install/broker /app

USER 65532:65532
EXPOSE 8081

ENTRYPOINT ["java", "-cp", "/app/lib/*", "no.kartverket.matrikkel.broker.MainKt"]
