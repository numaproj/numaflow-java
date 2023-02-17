# Java SDK for Numaflow

This SDK provides the interface for
writing [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/)
and [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in Java.

## Usage

For usage, see [examples](examples).

You will see a warning in the log on startup, which you can safely ignore:

```
Oct 25, 2022 12:26:30 PM io.netty.bootstrap.AbstractBootstrap setChannelOption
WARNING: Unknown channel option 'SO_KEEPALIVE' for channel '[id: 0x6e9c19c7]'
```

This is due to grpc-netty trying to set SO_KEEPALIVE when it
shouldn't (https://github.com/grpc/grpc-java/blob/47ddfa4f205d4672e035c37349dfd3036c74efb6/netty/src/main/java/io/grpc/netty/NettyClientTransport.java#L237)
.

## Runtime Requirements

* `java` 11+
* `x86_64 linux`

OS other than `x86_64 linux` can be supported with minor changes,
ref: https://netty.io/wiki/native-transports.html.
Consider building for multiple OS if needed.
Once grpc-netty depends on netty 5+, consider requiring newer java (
ref: https://github.com/netty/netty/issues/10991).

## Build Requirements

* `java` 11+
* `maven` 3.6+

## Build

```bash
mvn clean install
```

## Updating proto definition files

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
