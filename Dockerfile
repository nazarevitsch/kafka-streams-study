FROM amazoncorretto:21-alpine-jdk
COPY ./build/libs/kafka-stream-0.0.1-SNAPSHOT.jar .
CMD ["java","-jar","kafka-stream-0.0.1-SNAPSHOT.jar"]