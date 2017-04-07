# Akka Persistence Hazelcast
[![Build Status](https://travis-ci.org/sorokinigor/akka-persistence-hazelcast.svg?branch=master)](https://travis-ci.org/sorokinigor/akka-persistence-hazelcast)
[![codecov](https://codecov.io/gh/sorokinigor/akka-persistence-hazelcast/branch/master/graph/badge.svg)](https://codecov.io/gh/sorokinigor/akka-persistence-hazelcast)
[![Release 2.12](https://img.shields.io/maven-central/v/com.github.sorokinigor/akka-persistence-hazelcast_2.12.svg?label=release%20for%20Scala%202.12)](http://search.maven.org/#artifactdetails%7Ccom.github.sorokinigor%7Cakka-persistence-hazelcast_2.12%7C1.0.0%7Cjar)
[![Release 2.12](https://img.shields.io/maven-central/v/com.github.sorokinigor/akka-persistence-hazelcast_2.11.svg?label=release%20for%20Scala%202.11)](http://search.maven.org/#artifactdetails%7Ccom.github.sorokinigor%7Cakka-persistence-hazelcast_2.11%7C1.0.0%7Cjar)
## Introduction
A plugin for [Akka Persistence](http://doc.akka.io/docs/akka/2.4/scala/persistence.html), which provides a journal
and a snapshot store backed by Hazelcast. Please, consider to read the documentation for the [plugin](https://github.com/sorokinigor/akka-persistence-hazelcast/wiki) 
and the [Hazelcast](https://hazelcast.org/documentation/) itself.
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Activate plugin](#activate-plugin)

## Prerequisites
The plugin is [tested](https://travis-ci.org/sorokinigor/akka-persistence-hazelcast) against:
* Scala 2.11/2.12
* Java 8
* Hazelcast 3.6+
* Akka  2.4.0+

## Installation
### Dependencies
The plugin works with Scala 2.11/2.12, Hazelcast 3.6/3.7 and Akka 2.4.0+, but does not define them as compile time 
dependencies. Therefore, please, make sure that you have included all the relevant dependencies in your project.

All of the examples below are for Scala 2.12. You can easily change `2.12` to `2.11` in order to get the right artifacts.
### Gradle
```Groovy
dependencies {
    compile "com.github.sorokinigor:akka-persistence-hazelcast_2.12:1.0.0"
    compile "org.scala-lang:scala-library:2.12.1"
    compile "com.typesafe.akka:akka-persistence_2.12:2.4.16"
    compile "com.hazelcast:hazelcast:3.7.4"
}

repositories {
    mavenCentral()
}
```
### Sbt
```Scala
libraryDependencies += "com.github.sorokinigor" % "akka-persistence-hazelcast_2.12" % "1.0.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.1"
libraryDependencies += "com.typesafe.akka" % "akka-persistence_2.12" % "2.4.16"
libraryDependencies += "com.hazelcast" % "hazelcast" % "3.7.4" 
```
### Maven
```xml
<dependency>
  <groupId>com.github.sorokinigor</groupId>
  <artifactId>akka-persistence-hazelcast_2.12</artifactId>
  <version>1.0.0</version>
</dependency>

<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>2.12.1</version>
</dependency>

<dependency>
    <groupId>com.typesafe.akka</groupId>
    <artifactId>akka-persistence_2.12</artifactId>
    <version>2.4.16</version>
</dependency>

<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>3.7.4</version>
</dependency>
```
## Activate plugin 
Put this in a application.conf:
```
akka.persistence {
  journal.plugin = "hazelcast.journal"
  snapshot-store.plugin = "hazelcast.snapshot-store"
}
```
See more about the configuration in the [reference.conf](https://github.com/sorokinigor/akka-persistence-hazelcast/blob/master/src/main/resources/reference.conf), 
[documentation](https://github.com/sorokinigor/akka-persistence-hazelcast/wiki/Configuration) and
[unit tests](https://github.com/sorokinigor/akka-persistence-hazelcast/tree/master/src/test/resources).
