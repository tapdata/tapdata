package io.tapdata.pdk.apis.error;

public class NotSupportedException extends RuntimeException {
  public NotSupportedException() {
    super("Not supported");
  }
}
