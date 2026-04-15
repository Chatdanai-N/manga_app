# --- Stage 1: Build Stage ---
# ใช้ Image Maven ร่วมกับ Java 21 เพื่อทำการ Compile และ Build JAR
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app

# 1. Copy ไฟล์ pom.xml เพื่อทำการดาวน์โหลด Dependency ก่อน (ช่วยเรื่อง Cache)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# 2. Copy Source Code ทั้งหมดและทำการ Build
COPY src ./src
RUN mvn clean package -DskipTests

# --- Stage 2: Run Stage ---
# ใช้เฉพาะ JRE (Java Runtime) เพื่อรันแอปพลิเคชัน (ทำให้ Image เล็กและปลอดภัยขึ้น)
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# 3. Copy เฉพาะไฟล์ .jar ที่ Build เสร็จแล้วจาก Stage แรกมา
# ตรวจสอบชื่อไฟล์ .jar ใน target ให้ตรงกับใน pom.xml (artifactId-version.jar)
COPY --from=build /app/target/*.jar app.jar

# 4. ตั้งค่า Environment (ถ้ามี)
ENV JAVA_OPTS="-Xms512m -Xmx512m"

# 5. เปิด Port ตามที่ตั้งไว้ใน application.yml (ตัวอย่างคือ 8080)
EXPOSE 9090

# 6. คำสั่งรันแอปพลิเคชัน
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]