package io.tapdata.flow.engine.V2.script;

import com.tapdata.processor.ScriptLogger;
import io.tapdata.observable.logging.ObsLogger;

public class ObsScriptLogger implements ScriptLogger {

  private final ObsLogger obsLogger;

  public ObsScriptLogger(ObsLogger obsLogger) {
    this.obsLogger = obsLogger;
  }

  @Override
  public void debug(String message, Object... params) {
    obsLogger.debug(message, params);
  }

  @Override
  public void info(CharSequence message) {
    obsLogger.info(String.valueOf(message));
  }

  @Override
  public void info(String message, Object... params) {
    obsLogger.info(message, params);
  }

  @Override
  public void warn(String message, Object... params) {
    obsLogger.warn(message, params);
  }

  @Override
  public void error(String message, Object... params) {
    obsLogger.error(message, params);
  }

  @Override
  public void error(String message, Throwable throwable) {
    obsLogger.error(message, throwable);
  }

  @Override
  public void fatal(String message, Object... params) {
    obsLogger.error(message, params);
  }
}
