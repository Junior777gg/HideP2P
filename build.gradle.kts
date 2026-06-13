plugins {
    kotlin("plugin.serialization") version "2.3.0"
    kotlin("jvm") version "2.3.0"
}

group = "org.unstabledev"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    compileOnly("com.google.crypto.tink:tink:1.21.0")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(22)
}

tasks.test {
    useJUnitPlatform()
}