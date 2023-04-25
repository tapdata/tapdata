package io.tapdata.task.skiperrorevent;

import org.apache.logging.log4j.*;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.MarkerFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.EntryMessage;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;

import java.util.Optional;
import java.util.function.BiFunction;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/23 17:14 Create
 */
public class SplitFileLogger implements AutoCloseable, Logger {
  private final static String loggerName = SplitFileLogger.class.getName();
  private static LoggerConfig loggerConfig;
  private static Configuration config;
  private static String rootPath;

  public static void init(String rootPath, Level rootLevel) {
    init(rootPath, (loggerName, config) -> LoggerConfig.createLogger(
      false, rootLevel, loggerName, "TRUE", new AppenderRef[]{
      }, null, config, null
    ));
  }

  public static void init(String rootPath, BiFunction<String, Configuration, LoggerConfig> createConfigFn) {
    if (null == SplitFileLogger.loggerConfig) {
      LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

      SplitFileLogger.rootPath = Optional.ofNullable(rootPath).map(s -> {
        while (s.endsWith("/") || s.endsWith("\\")) {
          s = s.substring(0, s.length() - 1);
        }
        return s;
      }).orElse(".");
      SplitFileLogger.config = ctx.getConfiguration();
      SplitFileLogger.loggerConfig = createConfigFn.apply(loggerName, SplitFileLogger.config);
      SplitFileLogger.config.addLogger(loggerName, loggerConfig);
      ctx.updateLoggers();
    }
  }

  protected final String id;
  protected final Logger logger;
  protected final Appender appender;
  protected final Marker marker;

  public SplitFileLogger(Level level, String id) {
    if (null == loggerConfig) {
      throw new IllegalStateException("Please call init before new instance");
    }

    this.id = id;
    this.appender = getAppender();
    try {
      loggerConfig.addAppender(this.appender, level, getMakerFilter());
      this.appender.start();
      this.logger = LogManager.getLogger(loggerName);
      this.marker = MarkerManager.getMarker(getMaker());
    } catch (RuntimeException e) {
      try {
        this.appender.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      throw e;
    }
  }

  @Override
  public void close() throws Exception {
    loggerConfig.removeAppender(appender.getName());
    appender.stop();
  }

  protected String getMaker() {
    return String.format("maker-%s-skipErrorEvent", id);
  }

  protected MarkerFilter getMakerFilter() {
    return MarkerFilter.createFilter(getMaker(), Filter.Result.ACCEPT, Filter.Result.DENY);
  }

  protected String getAppenderName() {
    return String.format("appender-%s-skipErrorEvent", id);
  }

  protected String getAppenderPath() {
    return String.format("%s/%s-skipErrorEvent.log", rootPath, id);
  }

  protected Appender getAppender() {
    TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
    SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy("1GB");
    CompositeTriggeringPolicy compositeTriggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy, sizeBasedTriggeringPolicy);
    DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
      .withMax("30")
      .withConfig(config).build();

    String appendPath = getAppenderPath();
    return RollingFileAppender.newBuilder()
      .setName(getAppenderName())
      .withFileName(appendPath)
      .withFilePattern(appendPath + ".%d{yyyyMMdd}.gz")
      .setLayout(PatternLayout.newBuilder()
        .withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} %msg%n")
        .build())
      .withPolicy(compositeTriggeringPolicy)
      .withStrategy(strategy)
      .build();
  }

  // --- 以下为重写方法 ---

  @Override
  public void catching(Level level, Throwable throwable) {
    this.logger.catching(level, throwable);
  }

  @Override
  public void catching(Throwable throwable) {
    this.logger.catching(throwable);
  }

  @Override
  public void debug(Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Message message) {
    this.logger.debug(message);
  }

  @Override
  public void debug(Message message, Throwable throwable) {
    this.logger.debug(marker, message, throwable);
  }

  @Override
  public void debug(MessageSupplier messageSupplier) {
    this.logger.debug(marker, messageSupplier);
  }

  @Override
  public void debug(MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.debug(marker, messageSupplier, throwable);
  }

  @Override
  public void debug(CharSequence message) {
    this.logger.debug(marker, message);
  }

  @Override
  public void debug(CharSequence message, Throwable throwable) {
    this.logger.debug(marker, message, throwable);
  }

  @Override
  public void debug(Object message) {
    this.logger.debug(marker, message);
  }

  @Override
  public void debug(Object message, Throwable throwable) {
    this.logger.debug(marker, message, throwable);
  }

  @Override
  public void debug(String message) {
    this.logger.debug(marker, message);
  }

  @Override
  public void debug(String message, Object... params) {
    this.logger.debug(marker, message, params);
  }

  @Override
  public void debug(String message, Supplier<?>... paramSuppliers) {
    this.logger.debug(marker, message, paramSuppliers);
  }

  @Override
  public void debug(String message, Throwable throwable) {
    this.logger.debug(marker, message, throwable);
  }

  @Override
  public void debug(Supplier<?> messageSupplier) {
    this.logger.debug(marker, messageSupplier);
  }

  @Override
  public void debug(Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.debug(marker, messageSupplier, throwable);
  }

  @Override
  public void debug(Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void debug(String message, Object p0) {
    this.logger.debug(marker, message, p0);
  }

  @Override
  public void debug(String message, Object p0, Object p1) {
    this.logger.debug(marker, message, p0, p1);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2) {
    this.logger.debug(marker, message, p0, p1, p2);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.debug(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.debug(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.debug(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.debug(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void entry() {
    this.logger.entry();
  }

  @Override
  public void entry(Object... params) {
    this.logger.entry(params);
  }

  @Override
  public void error(Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Message message) {
    this.logger.error(marker, message);
  }

  @Override
  public void error(Message message, Throwable throwable) {
    this.logger.error(marker, message, throwable);
  }

  @Override
  public void error(MessageSupplier messageSupplier) {
    this.logger.error(marker, messageSupplier);
  }

  @Override
  public void error(MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.error(marker, messageSupplier, throwable);
  }

  @Override
  public void error(CharSequence message) {
    this.logger.error(marker, message);
  }

  @Override
  public void error(CharSequence message, Throwable throwable) {
    this.logger.error(marker, message, throwable);
  }

  @Override
  public void error(Object message) {
    this.logger.error(marker, message);
  }

  @Override
  public void error(Object message, Throwable throwable) {
    this.logger.error(marker, message, throwable);
  }

  @Override
  public void error(String message) {
    this.logger.error(marker, message);
  }

  @Override
  public void error(String message, Object... params) {
    this.logger.error(marker, message, params);
  }

  @Override
  public void error(String message, Supplier<?>... paramSuppliers) {
    this.logger.error(marker, message, paramSuppliers);
  }

  @Override
  public void error(String message, Throwable throwable) {
    this.logger.error(marker, message, throwable);
  }

  @Override
  public void error(Supplier<?> messageSupplier) {
    this.logger.error(marker, messageSupplier);
  }

  @Override
  public void error(Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.error(marker, messageSupplier, throwable);
  }

  @Override
  public void error(Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(String message, Object p0) {
    this.logger.error(marker, message, p0);
  }

  @Override
  public void error(String message, Object p0, Object p1) {
    this.logger.error(marker, message, p0, p1);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2) {
    this.logger.error(marker, message, p0, p1, p2);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.error(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.error(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.error(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.error(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void exit() {
    this.logger.exit();
  }

  @Override
  public <R> R exit(R result) {
    return this.exit(result);
  }

  @Override
  public void fatal(Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Message message) {
    this.logger.fatal(marker, message);
  }

  @Override
  public void fatal(Message message, Throwable throwable) {
    this.logger.fatal(marker, message, throwable);
  }

  @Override
  public void fatal(MessageSupplier messageSupplier) {
    this.logger.fatal(marker, messageSupplier);
  }

  @Override
  public void fatal(MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.fatal(marker, messageSupplier, throwable);
  }

  @Override
  public void fatal(CharSequence message) {
    this.logger.fatal(marker, message);
  }

  @Override
  public void fatal(CharSequence message, Throwable throwable) {
    this.logger.fatal(marker, message, throwable);
  }

  @Override
  public void fatal(Object message) {
    this.logger.fatal(marker, message);
  }

  @Override
  public void fatal(Object message, Throwable throwable) {
    this.logger.fatal(marker, message, throwable);
  }

  @Override
  public void fatal(String message) {
    this.logger.fatal(marker, message);
  }

  @Override
  public void fatal(String message, Object... params) {
    this.logger.fatal(marker, message, params);
  }

  @Override
  public void fatal(String message, Supplier<?>... paramSuppliers) {
    this.logger.fatal(marker, message, paramSuppliers);
  }

  @Override
  public void fatal(String message, Throwable throwable) {
    this.logger.fatal(marker, message, throwable);
  }

  @Override
  public void fatal(Supplier<?> messageSupplier) {
    this.logger.fatal(marker, messageSupplier);
  }

  @Override
  public void fatal(Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.fatal(marker, messageSupplier, throwable);
  }

  @Override
  public void fatal(Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fatal(String message, Object p0) {
    this.logger.fatal(marker, message, p0);
  }

  @Override
  public void fatal(String message, Object p0, Object p1) {
    this.logger.fatal(marker, message, p0, p1);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2) {
    this.logger.fatal(marker, message, p0, p1, p2);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.fatal(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.fatal(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.fatal(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.fatal(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public Level getLevel() {
    return this.logger.getLevel();
  }

  @Override
  public <MF extends MessageFactory> MF getMessageFactory() {
    return this.logger.getMessageFactory();
  }

  @Override
  public String getName() {
    return this.logger.getName();
  }

  @Override
  public void info(Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Message message) {
    this.logger.info(marker, message);
  }

  @Override
  public void info(Message message, Throwable throwable) {
    this.logger.info(marker, message, throwable);
  }

  @Override
  public void info(MessageSupplier messageSupplier) {
    this.logger.info(marker, messageSupplier);
  }

  @Override
  public void info(MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.info(marker, messageSupplier, throwable);
  }

  @Override
  public void info(CharSequence message) {
    this.logger.info(marker, message);
  }

  @Override
  public void info(CharSequence message, Throwable throwable) {
    this.logger.info(marker, message, throwable);
  }

  @Override
  public void info(Object message) {
    this.logger.info(marker, message);
  }

  @Override
  public void info(Object message, Throwable throwable) {
    this.logger.info(marker, message, throwable);
  }

  @Override
  public void info(String message) {
    this.logger.info(marker, message);
  }

  @Override
  public void info(String message, Object... params) {
    this.logger.info(marker, message, params);
  }

  @Override
  public void info(String message, Supplier<?>... paramSuppliers) {
    this.logger.info(marker, message, paramSuppliers);
  }

  @Override
  public void info(String message, Throwable throwable) {
    this.logger.info(marker, message, throwable);
  }

  @Override
  public void info(Supplier<?> messageSupplier) {
    this.logger.info(marker, messageSupplier);
  }

  @Override
  public void info(Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.info(marker, messageSupplier, throwable);
  }

  @Override
  public void info(Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void info(String message, Object p0) {
    this.logger.info(marker, message, p0);
  }

  @Override
  public void info(String message, Object p0, Object p1) {
    this.logger.info(marker, message, p0, p1);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2) {
    this.logger.info(marker, message, p0, p1, p2);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.info(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.info(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.info(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.info(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public boolean isDebugEnabled() {
    return this.logger.isDebugEnabled(marker);
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEnabled(Level level) {
    return this.logger.isEnabled(level, marker);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isErrorEnabled() {
    return this.isErrorEnabled(marker);
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isFatalEnabled() {
    return this.logger.isFatalEnabled(marker);
  }

  @Override
  public boolean isFatalEnabled(Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInfoEnabled() {
    return this.isInfoEnabled(marker);
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTraceEnabled() {
    return this.isTraceEnabled(marker);
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWarnEnabled() {
    return this.isWarnEnabled(marker);
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Message message) {
    this.logger.log(level, marker, message);
  }

  @Override
  public void log(Level level, Message message, Throwable throwable) {
    this.logger.log(level, marker, message, throwable);
  }

  @Override
  public void log(Level level, MessageSupplier messageSupplier) {
    this.logger.log(level, marker, messageSupplier);
  }

  @Override
  public void log(Level level, MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.log(level, marker, messageSupplier, throwable);
  }

  @Override
  public void log(Level level, CharSequence message) {
    this.logger.log(level, marker, message);
  }

  @Override
  public void log(Level level, CharSequence message, Throwable throwable) {
    this.logger.log(level, marker, message, throwable);
  }

  @Override
  public void log(Level level, Object message) {
    this.logger.log(level, marker, message);
  }

  @Override
  public void log(Level level, Object message, Throwable throwable) {
    this.logger.log(level, marker, message, throwable);
  }

  @Override
  public void log(Level level, String message) {
    this.logger.log(level, marker, message);
  }

  @Override
  public void log(Level level, String message, Object... params) {
    this.logger.log(level, marker, message, params);
  }

  @Override
  public void log(Level level, String message, Supplier<?>... paramSuppliers) {
    this.logger.log(level, marker, message, paramSuppliers);
  }

  @Override
  public void log(Level level, String message, Throwable throwable) {
    this.logger.log(level, marker, message, throwable);
  }

  @Override
  public void log(Level level, Supplier<?> messageSupplier) {
    this.logger.log(level, marker, messageSupplier);
  }

  @Override
  public void log(Level level, Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.log(level, marker, messageSupplier, throwable);
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void log(Level level, String message, Object p0) {
    this.logger.log(level, marker, message, p0);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1) {
    this.logger.log(level, marker, message, p0, p1);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2) {
    this.logger.log(level, marker, message, p0, p1, p2);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.log(level, marker, message, p0, p1, p2, p3);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.log(level, marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.log(level, marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void log(Level level, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.log(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void printf(Level level, Marker marker, String format, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void printf(Level level, String format, Object... params) {
    this.logger.printf(level, marker, format, params);
  }

  @Override
  public <T extends Throwable> T throwing(Level level, T throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Throwable> T throwing(T throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Message message) {
    this.logger.trace(marker, message);
  }

  @Override
  public void trace(Message message, Throwable throwable) {
    this.logger.trace(marker, message, throwable);
  }

  @Override
  public void trace(MessageSupplier messageSupplier) {
    this.logger.trace(marker, messageSupplier);
  }

  @Override
  public void trace(MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.trace(marker, messageSupplier, throwable);
  }

  @Override
  public void trace(CharSequence message) {
    this.logger.trace(marker, message);
  }

  @Override
  public void trace(CharSequence message, Throwable throwable) {
    this.logger.trace(marker, message, throwable);
  }

  @Override
  public void trace(Object message) {
    this.logger.trace(marker, message);
  }

  @Override
  public void trace(Object message, Throwable throwable) {
    this.logger.trace(marker, message, throwable);
  }

  @Override
  public void trace(String message) {
    this.logger.trace(marker, message);
  }

  @Override
  public void trace(String message, Object... params) {
    this.logger.trace(marker, message, params);
  }

  @Override
  public void trace(String message, Supplier<?>... paramSuppliers) {
    this.logger.trace(marker, message, paramSuppliers);
  }

  @Override
  public void trace(String message, Throwable throwable) {
    this.logger.trace(marker, message, throwable);
  }

  @Override
  public void trace(Supplier<?> messageSupplier) {
    this.logger.trace(marker, messageSupplier);
  }

  @Override
  public void trace(Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.trace(marker, messageSupplier, throwable);
  }

  @Override
  public void trace(Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void trace(String message, Object p0) {
    this.logger.trace(marker, message, p0);
  }

  @Override
  public void trace(String message, Object p0, Object p1) {
    this.logger.trace(marker, message, p0, p1);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2) {
    this.logger.trace(marker, message, p0, p1, p2);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.trace(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.trace(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.trace(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.trace(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public EntryMessage traceEntry() {
    throw new UnsupportedOperationException();
  }

  @Override
  public EntryMessage traceEntry(String format, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EntryMessage traceEntry(Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EntryMessage traceEntry(String format, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EntryMessage traceEntry(Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void traceExit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> R traceExit(R result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> R traceExit(String format, R result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void traceExit(EntryMessage message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> R traceExit(EntryMessage message, R result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> R traceExit(Message message, R result) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, Message message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, Message message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, MessageSupplier messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, MessageSupplier messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, CharSequence message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, CharSequence message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, Object message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, Object message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Supplier<?>... paramSuppliers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, Supplier<?> messageSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, Supplier<?> messageSupplier, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Message message) {
    this.logger.warn(marker, message);
  }

  @Override
  public void warn(Message message, Throwable throwable) {
    this.logger.warn(marker, message, throwable);
  }

  @Override
  public void warn(MessageSupplier messageSupplier) {
    this.logger.warn(marker, messageSupplier);
  }

  @Override
  public void warn(MessageSupplier messageSupplier, Throwable throwable) {
    this.logger.warn(marker, messageSupplier, throwable);
  }

  @Override
  public void warn(CharSequence message) {
    this.logger.warn(marker, message);
  }

  @Override
  public void warn(CharSequence message, Throwable throwable) {
    this.logger.warn(marker, message, throwable);
  }

  @Override
  public void warn(Object message) {
    this.logger.warn(marker, message);
  }

  @Override
  public void warn(Object message, Throwable throwable) {
    this.logger.warn(marker, message, throwable);
  }

  @Override
  public void warn(String message) {
    this.logger.warn(marker, message);
  }

  @Override
  public void warn(String message, Object... params) {
    this.logger.warn(marker, message, params);
  }

  @Override
  public void warn(String message, Supplier<?>... paramSuppliers) {
    this.logger.warn(marker, message, paramSuppliers);
  }

  @Override
  public void warn(String message, Throwable throwable) {
    this.logger.warn(marker, message, throwable);
  }

  @Override
  public void warn(Supplier<?> messageSupplier) {
    this.logger.warn(marker, messageSupplier);
  }

  @Override
  public void warn(Supplier<?> messageSupplier, Throwable throwable) {
    this.logger.warn(marker, messageSupplier, throwable);
  }

  @Override
  public void warn(Marker marker, String message, Object p0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(Marker marker, String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void warn(String message, Object p0) {
    this.logger.warn(marker, message, p0);
  }

  @Override
  public void warn(String message, Object p0, Object p1) {
    this.logger.warn(marker, message, p0, p1);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2) {
    this.logger.warn(marker, message, p0, p1, p2);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3) {
    this.logger.warn(marker, message, p0, p1, p2, p3);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
    this.logger.warn(marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
    this.logger.warn(marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
    this.logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
    this.logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    this.logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    this.logger.warn(marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }
}
