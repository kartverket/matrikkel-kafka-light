FROM dhi.io/eclipse-temurin:27.0.1.1-alpine3.24@sha256:17cb4b82e960047fc0d04ed1247803128375305cb2b5e264237e7b200354975b AS runtime

COPY apps/broker/build/install/broker /app

USER 65532:65532
EXPOSE 8081

ENTRYPOINT ["java", "-cp", "/app/lib/*", "no.kartverket.matrikkel.broker.MainKt"]
