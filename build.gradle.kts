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
    apply(plugin = "java-library")
    apply(plugin = "io.spring.dependency-management")

    extensions.configure<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension> {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:$springBootVersion")
        }
    }

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_22
        targetCompatibility = JavaVersion.VERSION_22
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
