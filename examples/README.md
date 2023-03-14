## Build

The sdk artifacts are published as GitHub packages. Check the links below on how to use GitHub packages as dependencies in a Java Project.
- [Reference](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry)
- [Reference](https://github.com/orgs/community/discussions/26634#discussioncomment-3252638)
### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
  <groupId>io.numaproj.numaflow</groupId>
  <artifactId>numaflow-java</artifactId>
  <version>${latest}</version>
  <scope>compile</scope>
</dependency>
```

### Gradle users

Add this dependency to your project's build file:

```groovy
compile "io.numaproj.numaflow:numaflow-java:${latest}"
```

### Examples on how to write UDFs and UDSinks in Java

* **User Defined Function(UDF)**
  * [Map](src/main/java/io/numaproj/numaflow/examples/function/map)
  * [Reduce](src/main/java/io/numaproj/numaflow/examples/function/reduce)

* **User Defined Sink(UDSink)**
  * [Sink](src/main/java/io/numaproj/numaflow/examples/sink/simple)
