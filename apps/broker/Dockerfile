FROM dhi.io/eclipse-temurin:25.0.5.8-alpine3.24@sha256:8da329a70f7794dd59467841e7476fd96a22fa5d3146c36811d3589b5205a5d0 AS runtime

COPY apps/broker/build/install/broker /app

USER 65532:65532
EXPOSE 8081

ENTRYPOINT ["java", "-cp", "/app/lib/*", "no.kartverket.matrikkel.broker.MainKt"]
