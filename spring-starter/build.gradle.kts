// Spring Boot starter — wires the saga core into a Spring application context

plugins {
    `java-library`
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    api(project(":core"))
    implementation(project(":engines:in-memory"))
    implementation("org.springframework.boot:spring-boot-autoconfigure")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
