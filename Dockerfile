# Stage 1: Build stage
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY . .
# รัน maven build โดยข้ามการ test
RUN ./mvnw clean package -DskipTests

# Stage 2: Run stage
FROM eclipse-temurin:21-jre
WORKDIR /app
# ก๊อปปี้ไฟล์ .jar จาก build stage
COPY --from=build /app/target/manga_app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]