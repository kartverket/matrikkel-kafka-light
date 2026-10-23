FROM ghcr.io/kartverket/emudhra-images/jre/dhi:jre-25.0-alpine3.24@sha256:f0b12aab0f96059700032dada95e0bf85e8e0e7404d92246370a0a752ed21c4d

RUN addgroup -g 150 -S apprunner \
 && adduser -u 150 -S apprunner -G apprunner

COPY --chown=150:150 apps/broker/build/install/broker /app

USER apprunner
EXPOSE 8081

ENTRYPOINT ["/app/bin/broker"]
