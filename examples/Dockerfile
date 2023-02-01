# syntax=docker/dockerfile:1

FROM --platform=linux/amd64 openjdk:11.0.15-jdk

WORKDIR app

COPY target/numaflow-java-examples-0.0.0-SNAPSHOT-jar-with-dependencies.jar ./

ENTRYPOINT ["java", "-jar", "numaflow-java-examples-0.0.0-SNAPSHOT-jar-with-dependencies.jar"]
