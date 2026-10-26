FROM ghcr.io/kartverket/emudhra-images/jre/dockerhub:jre-25.0-alpine3.24@sha256:2d2e8939adc7327871075062d4cca2dbd8da7542d903861dc9d0b84c96b3a722

RUN addgroup -g 150 -S apprunner \
 && adduser -u 150 -S apprunner -G apprunner

COPY --chown=150:150 apps/broker/build/install/broker /app

USER apprunner
EXPOSE 8081

ENTRYPOINT ["/app/bin/broker"]