import java.io.FileInputStream
import java.util.Properties

plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

val props = run {
    val projectProperties = Properties()
    val fis = FileInputStream("$rootDir/../gradle.properties")
    projectProperties.load(fis)
    projectProperties
}

fun version(k: String) =
    props.getProperty("versions.$k")

dependencies {
    implementation(libs.gradle.versions.plugin)
    implementation(libs.vanniktech.publish.plugin)
    implementation(libs.kotlin.gradle.plugin)
    implementation(libs.kover.gradle.plugin)
    implementation(libs.dokka.gradle.plugin)
    implementation(libs.detekt.gradle.plugin)
    implementation(libs.binary.compatibility.validator.plugin)
//    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.1.0")
//    implementation("io.gitlab.arturbosch.detekt:detekt-gradle-plugin:1.23.7")
//    implementation("org.jetbrains.dokka:dokka-gradle-plugin:1.9.20")
//    implementation("com.vanniktech:gradle-maven-publish-plugin:0.30.0")
}

kotlin {
    jvmToolchain(21)
}
