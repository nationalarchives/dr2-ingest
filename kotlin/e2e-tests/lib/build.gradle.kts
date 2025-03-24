plugins {
    `java-library`
    kotlin("plugin.serialization") version "2.1.10"
    kotlin("jvm") version "2.1.20"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.2"))
    api(libs.aws.sdk.kotlin.cloudwatchlogs.jvm)
    api(libs.aws.sdk.kotlin.sqs.jvm)
    api(libs.aws.sdk.kotlin.s3.jvm)
    api(libs.aws.sdk.kotlin.dynamodb.jvm)
    api(libs.aws.sdk.kotlin.sfn.jvm)
    api(libs.com.typesafe.config)
    api(libs.org.jetbrains.kotlinx.kotlinx.serialization.json)
    api(libs.org.apache.commons.commons.compress)
    testImplementation(libs.org.junit.platform.junit.platform.suite)
    testImplementation(libs.org.junit.jupiter.junit.jupiter)
    testImplementation(libs.org.jetbrains.kotlin.kotlin.test)
    testImplementation(project(":lib"))
}

group = "uk.gov.nationalarchives"
version = "1.0.0-SNAPSHOT"
description = "e2etests"

tasks.test {
    useJUnitPlatform()
    testLogging.showStandardStreams = true
}
