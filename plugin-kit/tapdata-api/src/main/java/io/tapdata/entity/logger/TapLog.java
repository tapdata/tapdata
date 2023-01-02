package io.tapdata.entity.logger;

public class TapLog implements Log {
  private static final String TAG = "LOG";


  @Override
  public void debug(String message, Object... params) {
    TapLogger.debug(TAG, message, params);
  }

  @Override
  public void info(CharSequence message) {
    TapLogger.info(TAG, message.toString());
  }

  @Override
  public void info(String message, Object... params) {
    TapLogger.info(TAG, message, params);
  }

  @Override
  public void warn(String message, Object... params) {
    TapLogger.warn(TAG, message, params);
  }

  @Override
  public void error(String message, Object... params) {
    TapLogger.error(TAG, message, params);
  }

  @Override
  public void error(String message, Throwable throwable) {
    TapLogger.error(TAG, message, throwable);
  }

  @Override
  public void fatal(String message, Object... params) {
    TapLogger.fatal(TAG, message, params);
  }

}
