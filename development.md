# Development

This document explains the development process for the Numaflow Java SDK.

### Testing

After you make certain changes to the Java SDK implementation, you would like to use it to build a 
brand new UDF image, and run it on a pipeline to verify the image continues working well with the Numaflow platform.
In order to do this you can follow these steps:

1. Make the changes.
2. Update your local repository using maven, to reflect your latest changes. After this, ensure that the `pom.xml` file
in the examples directory is also using these changes.
3. Build your test images, and test them in a pipeline.

There exists a script `update_examples.sh`, which you can also use while testing, for your convenience.

If you would like to build and push a specific example, you can run:

```shell
./update_examples -bpe <example-image> -t <tag>
 ```
The -bpe flag builds and pushes to the remote registry. <example-image> must be
the id tag used within the execution element of the desired example. It is recommended to specify a tag,
like `test`, as the default is `stable`, which is used by the Github Actions CI. For example, in order to push
the example, for the reducer SDK, specifically the sum function, you would run:
```shell
./update_examples -bpe reduce-sum -t test
```
If you would like to build and push all the examples at once, you can run:
```shell
./update_examples -bp -t test
```
Both `bpe` and `bp` first build a local image with the naming convention 
`numaflow-java-examples/<example_id>:<tag>`, which then gets pushed as 
`quay.io/numaio/numaflow-java/<example_id>:<tag>`. If while testing, you would like 
to build your image locally, without pushing to quay.io, you can run the following in the 
examples directory:

```shell
mvn jib:dockerBuild@<example_id> -Ddocker.tag=<tag>
```

If you would like to build all example images without pushing, in the examples directory you can also run:
```shell
mvn clean install -Ddocker.tag=<tag>
```

### Deploying

After confirming that your changes pass local testing:

1. Clean up testing artifacts.
2. Create a PR. Once your PR has been merged a Github Actions workflow will be triggered to build, tag (with `stable`), and push
all example images, so that they are using the most up-to-date version of the SDK, i.e. the one including your 
changes.

### Before Release

Before releasing a new SDK version, make sure to update all references from the old version to the new one.
For example, the version in the `pom.xml` in the root and example directories. For [reference
](https://github.com/numaproj/numaflow-java/pull/89/files#diff-9c5fb3d1b7e3b0f54bc5c4182965c4fe1f9023d449017cece3005d3f90e8e4d8).

### Adding a New Example

If you add a new example, there are a few steps to follow in order for it to be used by the update script and the Docker
Publish workflow:

1. Add the example to the `pom.xml` file in the examples directory, within an execution element. Note that the
id tag you specify must be exactly the same as the quay.io repository name for the example. 
2. Add the id tag you specified in step 1 to the executionIDs array in `update_examples.sh`.
3. Add the id tag you specified in step 1 to the execution_ids matrix in `build-push.yaml`.
