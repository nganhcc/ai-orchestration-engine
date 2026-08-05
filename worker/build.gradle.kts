plugins {
    id("org.springframework.boot") version "4.1.0"
    id("io.spring.dependency-management") version "1.1.7"
}

springBoot {
    mainClass.set("com.nganhcc.orchestration.worker.WorkerApplication")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":raft-core"))
    implementation(project(":rpc-transport"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    runtimeOnly("org.postgresql:postgresql")
}