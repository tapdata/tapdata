package io.tapdata.exception;

import io.tapdata.common.exception.TapdataException;
import io.tapdata.common.logging.error.ErrorCodeEnum;

public class SourceException extends TapdataException {

  private boolean needStop = false;

  public SourceException() {
    super();
  }

  public SourceException(String message, boolean needStop) {
    super(message);
    this.needStop = needStop;
  }

  public SourceException(String message, Throwable cause, boolean needStop) {
    super(message, cause);
    this.needStop = needStop;
  }

  public SourceException(Throwable cause, boolean needStop) {
    super(cause);
    this.needStop = needStop;
  }

  public boolean isNeedStop() {
    return needStop;
  }

  public SourceException configErrorCode(ErrorCodeEnum errorCode) {
    this.errorCode = errorCode;
    return this;
  }

}
