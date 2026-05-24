import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication

plugins {
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "hamburg.engelmann"
    version = project.findProperty("projectVersion") as? String ?: "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

val springBootVersion: String by project

val artifactIds = mapOf(
    "core"           to "saga-core",
    "in-memory"      to "saga-engine-in-memory",
    "persistent"     to "saga-engine-persistent",
    "spring-starter" to "saga-spring-starter"
)

subprojects {
    plugins.withId("java-library") {
        apply(plugin = "maven-publish")

        configure<JavaPluginExtension> {
            sourceCompatibility = JavaVersion.VERSION_25
            targetCompatibility = JavaVersion.VERSION_25
            withSourcesJar()
        }

        tasks.withType<Test> {
            useJUnitPlatform()
        }

        configure<PublishingExtension> {
            publications {
                create<MavenPublication>("mavenJava") {
                    artifactId = artifactIds[project.name] ?: "saga-${project.name}"
                    from(components["java"])
                    versionMapping {
                        usage("java-api") { fromResolutionOf("runtimeClasspath") }
                        usage("java-runtime") { fromResolutionResult() }
                    }
                    pom {
                        name.set(artifactIds[project.name] ?: project.name)
                        description.set("Reactive Saga orchestration library for Java")
                        url.set("https://github.com/TeddyBear2004/reactive-saga")
                        licenses {
                            license {
                                name.set("MIT License")
                                url.set("https://opensource.org/licenses/MIT")
                            }
                        }
                        scm {
                            connection.set("scm:git:git://github.com/TeddyBear2004/reactive-saga.git")
                            developerConnection.set("scm:git:ssh://github.com/TeddyBear2004/reactive-saga.git")
                            url.set("https://github.com/TeddyBear2004/reactive-saga")
                        }
                    }
                }
            }
            repositories {
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/TeddyBear2004/reactive-saga")
                    credentials {
                        username = System.getenv("GITHUB_ACTOR") ?: ""
                        password = System.getenv("GITHUB_TOKEN") ?: ""
                    }
                }
            }
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
