# Release Guide

This document explains the release process for the Java SDK. You can find the most recent version under [Github Releases](https://github.com/numaproj/numaflow-java/releases).

### Before Release

Before releasing a new SDK version, make sure to update all references from the old version to the new one.
For example, the version in the `README.md`, as well as the `pom.xml` in the root and example directories should be updated (for [reference
](https://github.com/numaproj/numaflow-java/pull/89/files#diff-9c5fb3d1b7e3b0f54bc5c4182965c4fe1f9023d449017cece3005d3f90e8e4d8)). After making these changes, create a PR. Once merged, it will trigger the Docker Publish workflow, and should be included in the release.
As a result, the correct SDK version will always be printed in the server information logs, and the example images will
always be using the latest changes (due to pointing to the local maven repository that is built).

### How to Release

This can be done via the Github UI. In the `Releases` section of the Java SDK repo, click `Draft a new release`. Create an appropriate tag for the new version and select it. Make
the title the same as the tag. Click `Generate release notes` so that all the changes made since the last release are documented. If there are any major features or breaking
changes that you would like to highlight as part of the release, add those to the description as well. Then set the release as either pre-release or latest, depending
on your situation. Finally, click `Publish release`, and your version tag will be the newest release on the repository (assuming that you chose `Set as the latest release`).

### After Release

After your release a Github Actions workflow, `Publish to Maven Central and Github Packages`, will be triggered. Monitor the workflow run and ensure that it succeeds.

If the released version has backwards incompatible changes, i.e. it does not support older versions of the Numaflow platform,
you must update the `MINIMUM_NUMAFLOW_VERSION` constant in the `src/main/java/io/numaproj/numaflow/info/ServerInfo.java` file to the minimum Numaflow version that is supported by your new SDK version. 
