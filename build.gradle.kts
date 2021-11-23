import org.gradle.api.publish.maven.MavenPom
import org.jetbrains.kotlin.gradle.plugin.getKotlinPluginVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val kotlinVersion = project.getKotlinPluginVersion()

plugins {
    kotlin("jvm") version "1.6.0"
    id("java")
    idea
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("maven-publish")
    id("com.jfrog.bintray") version "1.8.4"
    `java-library`
}

val publicationName = "ZZZ"

project.group = "com.walkmind"
val artifactID = "reactive-mysql-binlog-connector"
project.version = "1.25.5"
project.description = "MySQL Binary Log connector"
val licenseName = "Apache-2.0"
val licenseUrl = "http://opensource.org/licenses/apache-2.0"
val repoHttpsUrl = "https://github.com/unoexperto/reactive-mysql-binlog-connector.git"
val repoSshUri = "git@github.com:unoexperto/reactive-mysql-binlog-connector.git"

val awsCreds = File(System.getProperty("user.home") + "/.aws/credentials")
    .readLines()
    .map { it.trim() }.filter { it.isNotEmpty() && it.first() != '[' && it.last() != ']' && it.contains("=") }
    .map {
        val (k, v) = it.split("=").map { it.trim() }
        k.toLowerCase() to v
    }
    .toMap()

val sourcesJar by tasks.creating(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

val javadocJar by tasks.creating(Jar::class) {
    archiveClassifier.set("javadoc")
    from("$buildDir/javadoc")
}

val jar = tasks["jar"] as org.gradle.jvm.tasks.Jar

fun MavenPom.addDependencies() = withXml {
    asNode().appendNode("dependencies").let { depNode ->
        configurations.implementation.get().allDependencies.forEach {
            depNode.appendNode("dependency").apply {
                appendNode("groupId", it.group)
                appendNode("artifactId", it.name)
                appendNode("version", it.version)
            }
        }
    }
}

publishing {
    repositories {
        maven {
            url = uri("s3://${awsCreds["maven_bucket"]!!}/")
            credentials(AwsCredentials::class) {

                accessKey = awsCreds["aws_access_key_id"]
                secretKey = awsCreds["aws_secret_access_key"]
//                sessionToken = "someSTSToken" // optional
            }
        }
    }
    publications {
        create(publicationName, MavenPublication::class) {
            artifactId = artifactID
            groupId = project.group.toString()
            version = project.version.toString()
            description = project.description

            artifact(jar)
            artifact(sourcesJar) {
                classifier = "sources"
            }
            artifact(javadocJar) {
                classifier = "javadoc"
            }
            pom.addDependencies()
            pom {
                packaging = "jar"
                developers {
                    developer {
                        email.set("stanley.shyiko@gmail.com")
                        id.set("shyiko")
                        name.set("Stanley Shyiko")
                    }
                    developer {
                        email.set("ben@gimbo.net")
                        id.set("osheroff")
                        name.set("Ben Osheroff")
                    }
                    developer {
                        email.set("unoexperto.support@mailnull.com")
                        id.set("unoexperto")
                        name.set("ruslan")
                    }
                }
                licenses {
                    license {
                        name.set(licenseName)
                        url.set(licenseUrl)
                        distribution.set("repo")
                    }
                }
                scm {
                    connection.set("scm:$repoSshUri")
                    developerConnection.set("scm:$repoSshUri")
                    url.set(repoHttpsUrl)
                }
            }
        }
    }
}

idea {
    module {
        isDownloadJavadoc = true
        isDownloadSources = true
    }
}

dependencies {
    implementation(kotlin("stdlib-jdk8", kotlinVersion))
//    implementation(kotlin("reflect", kotlinVersion))

    compileOnly("io.projectreactor:reactor-core:3.4.12")

//    implementation("com.walkmind.extensions:serializers:1.5")
//    compileOnly("org.rocksdb:rocksdbjni:6.15.5")
//    compileOnly("io.netty:netty-buffer:4.1.60.Final")

    testImplementation("org.testng:testng:6.8.5")
    testImplementation("org.skyscreamer:jsonassert:1.4.0")
    testImplementation("org.mockito:mockito-all:1.9.5")
    testImplementation("mysql:mysql-connector-java:8.0.15")
    testImplementation("com.fasterxml.jackson.core:jackson-core:2.9.10")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.9.10.3")

//    testImplementation(kotlin("test-junit5", kotlinVersion))
//    testImplementation("com.walkmind.extensions:serializers:1.5")
//    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
//    testImplementation("net.jqwik:jqwik:1.3.0")
//    testImplementation("org.fusesource.leveldbjni:leveldbjni-all:1.8")
//    testImplementation("org.rocksdb:rocksdbjni:6.15.5")
//    testImplementation("io.netty:netty-buffer:4.1.60.Final")
}

repositories {
    mavenCentral()
    maven(url = "https://${awsCreds["maven_bucket"]!!}.s3.amazonaws.com/")
    maven ("https://repo.spring.io/snapshot")
    maven ("https://repo.spring.io/release")
    flatDir {
        dirs("libs")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
        kotlinOptions.freeCompilerArgs = listOf(
                "-Xjsr305=strict",
                "-Xjvm-default=enable",
                "-XXLanguage:+NewInference",
                "-Xinline-classes",
                "-Xjvm-default=enable")
        kotlinOptions.apiVersion = "1.6"
        kotlinOptions.languageVersion = "1.6"
    }

    withType<Test>().all {
//        jvmArgs = listOf("--enable-preview")
        testLogging.showStandardStreams = true
        testLogging.showExceptions = true
        useTestNG {
        }
    }

    withType<JavaExec>().all {
//        jvmArgs = listOf("--enable-preview")
    }

    withType<Wrapper>().all {
        gradleVersion = "7.1"
        distributionType = Wrapper.DistributionType.BIN
    }

    withType<JavaCompile>().all {
//        options.compilerArgs.addAll(listOf("--enable-preview", "-Xlint:preview"))
    }
}
