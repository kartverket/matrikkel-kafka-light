FROM ghcr.io/kartverket/emudhra-images/jre/dhi:jre-25.0-alpine3.24@sha256:f0b12aab0f96059700032dada95e0bf85e8e0e7404d92246370a0a752ed21c4d

COPY apps/broker/build/install/broker /app

EXPOSE 8081

ENTRYPOINT ["java", "-cp", "/app/lib/*", "no.kartverket.matrikkel.broker.MainKt"]
