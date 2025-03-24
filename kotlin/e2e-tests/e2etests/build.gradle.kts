plugins {
    `java-library`
    kotlin("jvm") version "2.1.20"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.2"))
    api(libs.com.typesafe.config)
    testImplementation(libs.io.cucumber.cucumber.java)
    testImplementation(libs.io.cucumber.cucumber.junit.platform.engine)
    testImplementation(libs.org.junit.platform.junit.platform.suite)
    testImplementation(libs.org.junit.jupiter.junit.jupiter)
    testImplementation(libs.org.jetbrains.kotlin.kotlin.test)
    testImplementation(project(":lib"))
}

tasks.test {
    useJUnitPlatform()
    testLogging.showStandardStreams = true
}
