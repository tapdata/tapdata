package io.tapdata.entity.logger;

public interface Log {

  void debug(String message, Object... params);

  // info level public logger api

  default void info(Object message) {
    info(message.toString());
  }

  default void info(String message) {
    info(message, new Object());
  }

  default void info(CharSequence message) {
    info(String.valueOf(message));
  }
  void info(String message, Object... params);

  // warn level public logger api

  default void warn(String message) {
    warn(message, new Object());
  }

  default void warn(CharSequence message) {
    warn(String.valueOf(message));
  }

  default void warn(Object message) {
    warn(message.toString());
  }
  void warn(String message, Object... params);


  default void error(CharSequence message) {
    error(String.valueOf(message));
  }

  default void error(String message) {
    error(message, new Object());
  }

  default void error(Object message) {
    error(message.toString());
  }
  void error(String message, Object... params);

  void error(String message, Throwable throwable);


  void fatal(String message, Object... params);

}
