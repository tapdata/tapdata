package com.tapdata.processor;

import io.tapdata.entity.logger.Log;
import org.apache.logging.log4j.Logger;

public class Log4jScriptLogger implements Log {

  private final Logger logger;

  public Log4jScriptLogger(Logger logger) {
    this.logger = logger;
  }

  @Override
  public void debug(String message, Object... params) {
    logger.debug(message, params);
  }

  @Override
  public void info(CharSequence message) {
    logger.info(message);
  }

  @Override
  public void info(String message, Object... params) {
    logger.info(message, params);
  }

  @Override
  public void warn(String message, Object... params) {
    logger.warn(message, params);
  }

  @Override
  public void error(String message, Object... params) {
    logger.error(message, params);
  }

  @Override
  public void error(String message, Throwable throwable) {
    logger.error(message, throwable);
  }

  @Override
  public void fatal(String message, Object... params) {
    logger.fatal(message, params);
  }

}
