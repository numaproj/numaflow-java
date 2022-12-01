## Build

The sdk artifacts aren't deployed anywhere, so ensure you have run `mvn clean install` on the sdk
first to ensure
artifacts are in your local `~/.m2/repository`. Alternatively publish them to a repository that
maven can access.

The build assumes a local docker daemon is running as it uses and loads images to it
named `numaflow-java-examples/<example-name>:latest` by default.

## Install

```bash
# Build
mvn clean install

# Either load to a local cluster, or alternatively push to a registry
IMAGE_REGISTRY="my.fake.registry"

docker tag numaflow-java-examples/forward-function:latest $IMAGE_REGISTRY/numaflow-java-examples/forward-function:latest
docker push $IMAGE_REGISTRY/numaflow-java-examples/forward-function:latest

docker tag numaflow-java-examples/simple-sink:latest $IMAGE_REGISTRY/numaflow-java-examples/simple-sink:latest
docker push $IMAGE_REGISTRY/numaflow-java-examples/simple-sink:latest

# For first-time install
kubectl apply -f example-java-pipeline.yaml

# Updates after re-build
kubectl delete po -l numaflow.numaproj.io/vertex-name=simple-sink
kubectl delete po -l numaflow.numaproj.io/vertex-name=forward-function
```

`example-java-pipeline.yaml`:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: example-java-pipeline
spec:
  vertices:
    - name: in
      source:
        generator: {}
    - name: forward-function
      udf:
        container:
          # Modify if necessary
          image: numaflow-java-examples/forward-function:latest
    - name: simple-sink
      sink:
        udsink:
          container:
            # Modify if necessary
            image: numaflow-java-examples/simple-sink:latest
  edges:
    - from: in
      to: forward-function
    - from: forward-function
      to: simple-sink
```

## Uninstall

```bash
# Cleanup cluster
kubectl delete -f example-java-pipeline.yaml

# Cleanup local machine
docker rmi numaflow-java-examples/forward-function:latest numaflow-java-examples/simple-sink:latest

# If you created other tags, also clean those up
docker rmi $IMAGE_REGISTRY/numaflow-java-examples/forward-function:latest $IMAGE_REGISTRY/numaflow-java-examples/simple-sink:latest
```
