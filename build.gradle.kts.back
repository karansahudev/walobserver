plugins {
    id("java")
    id("org.jetbrains.kotlin.jvm") version "1.9.0"
    id("application")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("org.example.KafkaWALObserver")
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "org.example.KafkaWALObserver"
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // HBase
    implementation("org.apache.hbase:hbase-client:2.4.9") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    implementation("org.apache.hbase:hbase-common:2.4.9") {
        exclude("org.slf4j", "slf4j-log4j12")
    }
    implementation("org.apache.hbase:hbase-server:2.4.9") {
        exclude("org.slf4j", "slf4j-log4j12")
    }

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.2.0") {
        exclude("org.slf4j", "slf4j-log4j12")
    }

    // AWS SDK
    implementation(platform("software.amazon.awssdk:bom:2.21.1"))

    // SnakeYAML
    implementation("org.yaml:snakeyaml:1.29")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("ch.qos.logback:logback-classic:1.2.6") {
        exclude("org.slf4j", "slf4j-api")
    }

}

tasks.test {
    useJUnitPlatform()
}


