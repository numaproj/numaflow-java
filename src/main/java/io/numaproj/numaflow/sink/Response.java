package io.numaproj.numaflow.sink;

public class Response {
  private final String id;
  private final Boolean success;
  private final String err;

  public Response(String id, Boolean success, String err) {
    this.id = id;
    this.success = success;
    this.err = err;
  }

  public String getId() {
    return id;
  }

  public Boolean getSuccess() {
    return success;
  }

  public String getErr() {
    return err;
  }
}
