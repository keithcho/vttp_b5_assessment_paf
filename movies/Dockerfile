FROM eclipse-temurin:23-noble AS builder

WORKDIR /src

COPY mvnw .
COPY pom.xml .

COPY .mvn .mvn
COPY src src

RUN chmod a+x mvnw && ./mvnw package -Dmaven.test.skip=true

FROM eclipse-temurin:23-jre-noble

WORKDIR /app

COPY --from=builder src/target/movies-0.0.1-SNAPSHOT.jar app.jar

RUN apt-get update && apt-get install -y curl

ENV PORT=8080

EXPOSE ${PORT}

ENTRYPOINT SERVER_PORT=${PORT} java -jar app.jar