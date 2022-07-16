package io.tapdata.exception;

public class DataFlowException extends RuntimeException {

  private boolean needStop = false;

  public DataFlowException() {
    super();
  }

  public DataFlowException(String message, boolean needStop) {
    super(message);
    this.needStop = needStop;
  }

  public DataFlowException(String message, Throwable cause, boolean needStop) {
    super(message, cause);
    this.needStop = needStop;
  }

  public DataFlowException(Throwable cause, boolean needStop) {
    super(cause);
    this.needStop = needStop;
  }

  public boolean isNeedStop() {
    return needStop;
  }
}
