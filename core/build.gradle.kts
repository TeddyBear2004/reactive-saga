// Pure saga engine — no Spring, no framework dependencies
// Depends only on Reactor, SLF4J API, jspecify, and Lombok

dependencies {
    api(libs.reactor.core)
    api("org.slf4j:slf4j-api")
    implementation(libs.jspecify)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testImplementation(libs.reactor.test)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("ch.qos.logback:logback-classic")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
