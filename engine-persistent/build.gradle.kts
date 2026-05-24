// Persistent saga engine — step-by-step execution state persisted to a repository,
// enabling crash recovery and durable distributed saga execution.
// Depends only on :core and Reactor (transitive).

plugins {
    `java-library`
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    api(project(":core"))
    implementation(libs.jspecify)
    implementation("com.fasterxml.jackson.core:jackson-databind")

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
