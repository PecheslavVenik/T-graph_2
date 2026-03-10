# syntax=docker/dockerfile:1.7

FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml mvnw mvnw.cmd ./
COPY .mvn .mvn
RUN chmod +x mvnw
RUN ./mvnw -q -DskipTests dependency:go-offline

COPY src src
RUN ./mvnw -q -DskipTests package

FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

RUN useradd --system --create-home --home-dir /app --shell /usr/sbin/nologin appuser \
    && mkdir -p /data/graph_api \
    && chown -R appuser:appuser /app /data/graph_api

COPY --from=build /workspace/target/graph_api_v2-*.jar /app/app.jar

ENV JAVA_OPTS=""
ENV SPRING_DATASOURCE_URL="jdbc:duckdb:/data/graph_api/graph.db"

VOLUME ["/data/graph_api"]
EXPOSE 8080

USER appuser
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
