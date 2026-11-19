# syntax=docker/dockerfile:1.4
ARG BASE_REGISTRY=third-party
FROM ${BASE_REGISTRY}/eclipse-temurin:17-jre

ARG JAR_FILE
LABEL authors="gbif"

RUN useradd -r -s /bin/false stackable
WORKDIR /app

COPY --chown=stackable ${JAR_FILE} /app/service.jar
COPY --chown=stackable docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# HTTP: the ws API and Actuator
EXPOSE 8080

ENV JVM_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Xms128m"

USER stackable
ENTRYPOINT ["/entrypoint.sh"]
