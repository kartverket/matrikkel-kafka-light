FROM ghcr.io/kartverket/emudhra-images/jre/dockerhub:jre-25.0-alpine3.24@sha256:a1a2a58677616ad8d737329c6c00e93d9a3c45661a0effc2bf7a93924c0b76fb

RUN addgroup -g 150 -S apprunner \
 && adduser -u 150 -S apprunner -G apprunner

COPY --chown=150:150 apps/broker/build/install/broker /app

USER apprunner
EXPOSE 8081

ENTRYPOINT ["/app/bin/broker"]