FROM openjdk:8-jdk-alpine
VOLUME /tmp
ADD ./resources/application.yml application.yml
ADD ./resources/log4j2.yml log4j2.yml
ADD embedded2Mongo.jar app.jar
RUN sh -c 'touch /app.jar'
ENV JAVA_OPTS="-Dspring.config.location=file:/application.yml -Dlogging.config=file:/log4j2.yml"
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app.jar > /dev/null" ]