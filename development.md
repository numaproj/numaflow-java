# Development

This document explains the development process for the Numaflow Java SDK.

### Testing

After you make certain changes to the Java SDK implementation, you would like to use it to build a 
brand new UDF image, and run it on a pipeline to verify the image continues working well with the Numaflow platform.
In order to do this you can follow these steps:

1. Make the changes.
2. First increase the version in the root `pom.xml` file (increasing the PATCH version by 1 is suggested, example: X.X.0 -> X.X.1).
This can be achieved by running the following at the root directory:
    ```shell
    ./update_examples -u <version>
    ```
3. You can now build the test UDF image and push it to a remote repository. You can do this by simply running:
    ```shell
    ./update_examples -bpe <example-image> -t <tag>
    ```
    the -bpe flag builds and pushes the given example to the remote registry, <example-image> must be
    the id tag used within the execution element of the desired example. It is recommended to specify a tag,
    like `test`, as the default is `stable` which is used by the Github Actions CI. For example in order to push
    the example for the reducer SDK, specifically the sum function, we would run:
    ```shell
    ./update_examples -bpe reduce-sum -t test
    ```
   If you would like to build and push all the examples at once, you can run:
   ```shell
    ./update_examples -bp -t test
    ```
4. Once the version is updated and the desired image is built, tagged, and pushed, you can run a pipeline 
using the image, i.e. we can use Numaflow e2e test cases to verify the changes.
   - Checkout the latest Numaflow repository
   - Find the test case and update the image and path in the test pipeline yaml file
   - Run `make Test*`

### Deploying

After confiring that your changes pass local testing, you should roll back the version updates before 
publishing the PR. The real version updates can be raised in a separate PR, which is discussed 
in the [After Release](#after-release) section. 

1. Revert both the `pom.xml` changes by re-running the update script with the version flag:
```shell
./update_examples -u <version-before-testing-update>
```
2. Clean up testing artifacts, i.e. delete any images that are no longer used on the remote registry repositories,
so that they are not flooded with testing images.
3. Create a PR. Once your PR has been merged a Github Actions workflow will be triggered to build, tag, and push
all example images, so that they are using the most up-to-date version of the SDK, i.e. the one including your 
changes.

### After Release

Once a new release has been made, and its corresponding version tag exists on the remote repo, we want to update the dependency
management files to reflect this new version:

```shell
./update_examples.sh -u <new-released-version>
  ```

Similar to the deployment step, create a PR for the changes created (which should just be the version bump
in the `pom.xml` files in root and example directories), and a Github Actions workflow will be triggered to
build, tag, and push all example images.
