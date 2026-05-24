// Stateless, in-memory saga engine — execution state lives entirely in the reactive chain.
// This is the default engine and the simplest option for applications that do not require
// crash-recovery or cross-node durability.

plugins {
    `java-library`
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    api(project(":core"))
    implementation(libs.jspecify)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(libs.reactor.test)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
