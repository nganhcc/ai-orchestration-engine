plugins {
    id("org.springframework.boot") version "4.1.0"
    id("io.spring.dependency-management") version "1.1.7"
    id("org.flywaydb.flyway") version "10.15.0"

}

springBoot {
    mainClass.set("com.nganhcc.orchestration.orchestrator.OrchestratorApplication")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":raft-core"))
    implementation(project(":rpc-transport"))
    implementation("io.netty:netty-all:4.1.115.Final")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.boot:spring-boot-flyway")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.flywaydb:flyway-core:10.15.0")
    implementation("org.flywaydb:flyway-database-postgresql:10.15.0")
    runtimeOnly("org.postgresql:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.mockito:mockito-core:5.6.0")
}

tasks.named<Jar>("jar") {
    enabled = false
}

