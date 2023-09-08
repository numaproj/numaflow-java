# Java SDK for Numaflow

[![Build](https://github.com/numaproj/numaflow-java/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/numaproj/numaflow-java/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow-java?label=numaflow-java)](https://github.com/numaproj/numaflow-java/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/io.numaproj.numaflow/numaflow-java.svg?label=Maven%20Central)](https://central.sonatype.com/search?q=numaflow+java&smo=true)

This SDK provides the interface for
writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/)
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Java.
.
## Getting Started

### Requirements

* `java` 11+
* `maven` 3.6+

### Importing Numaflow Java SDK

#### Maven users

Add this dependency to your project's POM:
```xml
<dependency>
  <groupId>io.numaproj.numaflow</groupId>
  <artifactId>numaflow-java</artifactId>
  <version>0.5.1</version>
</dependency>
```

#### Gradle users

Add this dependency to your project's build file:

```groovy
compile "io.numaproj.numaflow:numaflow-java:0.5.1"
```
```

### Build

```bash
mvn clean install
```

### Examples on how to write UDFs and UDSinks in Java

* **User Defined Function(UDF)**
    * [Map](src/main/java/io/numaproj/numaflow/examples/function/map)
    * [Reduce](src/main/java/io/numaproj/numaflow/examples/function/reduce)

* **User Defined Sink(UDSink)**
    * [Sink](src/main/java/io/numaproj/numaflow/examples/sink/simple)


You will see a warning in the log on startup, which you can safely ignore:

```
Oct 25, 2022 12:26:30 PM io.netty.bootstrap.AbstractBootstrap setChannelOption
WARNING: Unknown channel option 'SO_KEEPALIVE' for channel '[id: 0x6e9c19c7]'
```

This is due to grpc-netty trying to set SO_KEEPALIVE when it
shouldn't (https://github.com/grpc/grpc-java/blob/47ddfa4f205d4672e035c37349dfd3036c74efb6/netty/src/main/java/io/grpc/netty/NettyClientTransport.java#L237)

### API Documentation

Please, refer to our [Javadoc](https://javadoc.io/doc/io.numaproj.numaflow/numaflow-java/latest/index.html) website.

## Development

### Updating proto definition files

To keep up-to-date, do the following before building:

* copy the `*.proto` files
  from [numaflow-go](https://github.com/numaproj/numaflow-go/tree/main/pkg/apis/proto)
  into `/src/main/proto`
* replace the `go_package` lines with the following `java_package`:

```protobuf
option java_package = "io.numaproj.numaflow.function.v1";
```

```protobuf
option java_package = "io.numaproj.numaflow.sink.v1";
```

## Code Style

Use [Editor Config](https://www.jetbrains.com/help/idea/editorconfig.html). 
