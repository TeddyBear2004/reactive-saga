plugins {
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "com.saga"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

val springBootVersion: String by project

subprojects {
    plugins.withId("java-library") {
        configure<JavaPluginExtension> {
            sourceCompatibility = JavaVersion.VERSION_25
            targetCompatibility = JavaVersion.VERSION_25
        }
        tasks.withType<Test> {
            useJUnitPlatform()
        }
    }

    plugins.withId("io.spring.dependency-management") {
        extensions.configure<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension> {
            imports {
                mavenBom("org.springframework.boot:spring-boot-dependencies:$springBootVersion")
            }
        }
    }
}
