FROM dhi.io/eclipse-temurin:25.0.5.8.1.2-alpine3.24@sha256:f7e4086e2217b2804f273843311aceb8ff8a4a8e774e363171c7ae727d03fb29 AS runtime

COPY apps/broker/build/install/broker /app

USER 65532:65532
EXPOSE 8081

ENTRYPOINT ["java", "-cp", "/app/lib/*", "no.kartverket.matrikkel.broker.MainKt"]
