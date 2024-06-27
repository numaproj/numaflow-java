# Developer Guide

This document explains the development process for the Numaflow Java SDK.

### Testing

After you make certain changes to the Java SDK implementation, you would like to use it to build a 
brand new UDF image, and run it on a pipeline to verify the image continues working well with the Numaflow platform.
In order to do this you can follow these steps:

1. Make the changes
2. Update your local repository using maven, to reflect your latest changes. This can be done in the root
directory by running: `mvn clean install` (make sure that the version in the `pom.xml` in the root and examples
directories match)
3. In the examples directory you can then run `mvn clean install -Ddocker.tag=<tag>`, which will build `numaflow-java-examples`, run unit tests,
and then build all example images. Now that the images are available locally,
you can take the desired image and test it in a pipeline

You can build and push a specific example image by running the following:
```shell
./hack/update_examples.sh -bpe <example-execution-id> -t <tag>
 ```
The default tag is `stable`, but it is recommended you specify your own for testing purposes, as the Github Actions CI uses the `stable` tag.
This consistent tag name is used so that the tags in the [E2E test pipelines](https://github.com/numaproj/numaflow/tree/main/test) do not need to be
updated each time an SDK change is made.

`-bpe`first builds a local image with the naming convention 
`numaflow-java-examples/<example-execution-id>:<tag>`, which then gets pushed as 
`quay.io/numaio/numaflow-java/<example-execution-id>:<tag>`. If you want to build and tag all images locally, 
without pushing to quay.io, as mentioned in step 3, run: `mvn clean install -Ddocker.tag=<tag>`.

Note: before running the script, ensure that through the CLI, you are logged into quay.io. 

### Deploying

After confirming that your changes pass local testing:

1. Clean up testing artifacts
2. Create a PR. Once your PR has been merged, a Github Actions workflow (`Docker Publish`) will be triggered, to build, tag (with `stable`), and push
all example images. This ensures that all example images are using the most up-to-date version of the SDK, i.e. the one including your 
changes

### Adding a New Example

If you add a new example, there are a few steps to follow in order for it to be used by the update script and the 
`Docker Publish` workflow:

1. Add the example to the `examples/pom.xml`, within an execution element. Note that the
`id` tag you specify must be exactly the same as the quay.io repository name for the example
2. Add the `id` tag you specified in step 1 to the `executionIDs` array in `hack/update_examples.sh`
3. Add the `id` tag you specified in step 1 to the `execution_ids` matrix in `.github/workflows/build-push.yaml`
