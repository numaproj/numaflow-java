# syntax=docker/dockerfile:1

FROM --platform=linux/amd64 adoptopenjdk/openjdk11:jre-11.0.6_10-alpine

WORKDIR app

COPY target/numaflow-java-examples-0.0.0-SNAPSHOT-jar-with-dependencies.jar ./

ENTRYPOINT ["java", "-jar", "numaflow-java-examples-0.0.0-SNAPSHOT-jar-with-dependencies.jar"]
