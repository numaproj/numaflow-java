# SideInput Example

An example that demonstrates how to write a `sideinput` function along with a sample `User Defined function`
which watches and uses the corresponding side input.

### SideInput
```java
public class SimpleSideInput extends SideInputRetriever {
    private final Config config;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public SimpleSideInput(Config config) {
        this.config = config;
    }

    @Override
    public Message retrieveSideInput() {
        byte[] val;
        if (0.9 > config.getDropRatio()) {
            config.setDropRatio(0.5F);
        } else {
            config.setDropRatio(config.getDropRatio() + 0.01F);
        }
        try {
            val = jsonMapper.writeValueAsBytes(config);
            return Message.broadcastMessage(val);
        } catch (JsonProcessingException e) {
            return Message.noBroadcastMessage();
        }
    }

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSideInput(new Config("sampling", 0.5F))).start();
    }
}
```
After performing the retrieval/update for the side input value the user can choose to either broadcast the
message to other side input vertices or drop the message. The side input message is not retried.

For each side input there will be a file with the given path and after any update to the side input value the file will
be updated.

The directory is fixed and can be accessed through constants `Constants.SIDE_INPUT_DIR`.
The file name is the name of the side input.
```text
Constants.SIDE_INPUT_DIR -> "/var/numaflow/side-inputs"
sideInputFileName -> "/var/numaflow/side-inputs/sideInputName"
```

### User Defined Function

The UDF vertex will watch for changes to this file and whenever there is a change it will read the file to obtain the new side input value.

### Pipeline spec

In the spec we need to define the side input vertex and the UDF vertex. The UDF vertex will have the side input vertex as a side input.

Side input spec:
```yaml
spec:
  sideInputs:
    - name: myticker
      container:
        image: "quay.io/numaio/numaflow-java/sideinput:v0.5.0"
        imagePullPolicy: Always
      trigger:
        schedule: "*/2 * * * *"

```

Vertex spec for the UDF vertex:
```yaml
    - name: si-log
      udf:
        container:
          image: "quay.io/numaio/numaflow-java/udf-sideinput:v0.5.0"
          imagePullPolicy: Always
      containerTemplate:
        env:
          - name: NUMAFLOW_DEBUG
            value: "true" # DO NOT forget the double quotes!!!
      sideInputs:
        - myticker
```
