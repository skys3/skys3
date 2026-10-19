# The SDK matrix's Java client (tests/sdk/run.sh). The SDK and plugin
# versions are pinned in pom.xml.
FROM maven:3.9.16-eclipse-temurin-21@sha256:99e61abcff91a9b1333463bd8451fb18495d6eba9250ac66a338b518f8278320
WORKDIR /client
COPY pom.xml .
COPY src src
RUN mvn -q -B package && cp target/sdk-matrix.jar /client/sdk-matrix.jar && rm -rf target /root/.m2
ENTRYPOINT ["java", "-jar", "/client/sdk-matrix.jar"]
