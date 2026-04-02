FROM maven:3.9.6-eclipse-temurin-21-alpine as builder
WORKDIR /app
COPY libs/daomodel-0.01.jar /app/libs/daomodel-0.01.jar
RUN mvn install:install-file \
    -Dfile=/app/libs/daomodel-0.01.jar \
    -DgroupId=com.gnm.rsc \
    -DartifactId=daomodel \
    -Dversion=0.01 \
    -Dpackaging=jar
COPY src/ src
COPY pom.xml ./
RUN mvn clean package -DskipTests
CMD ["java", "-jar", "target/reportservice-0.0.1-SNAPSHOT.jar"]

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/reportservice-0.0.1-SNAPSHOT.jar .
CMD ["java", "-jar", "reportservice-0.0.1-SNAPSHOT.jar"]
