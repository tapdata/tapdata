package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Version {

  private static final Logger log = LogManager.getLogger(Version.class);

  private static String VERSION;

  private Version() {
  }


  static {
    StringBuilder sb = new StringBuilder();
    String version = System.getenv("version");
    String javaVersion = System.getProperty("java.version");
    if (StringUtils.isNotEmpty(version)) {
      sb.append("FlowEngine/").append(version).append(" ").append("java/").append(javaVersion);
    }
    VERSION = sb.toString();
    log.info("flow engine version: {}", VERSION);
  }

  public static String get() {
    return VERSION;
  }
}
