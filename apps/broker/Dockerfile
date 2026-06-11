FROM eclipse-temurin:25-jre-alpine@sha256:c707c0d18cb9e8556380719f80d96a7529d0746fbb42143893949b98ed2f8943

COPY apps/broker/build/install/broker /app

USER nonroot
EXPOSE 8081

ENTRYPOINT ["/app/bin/broker"]