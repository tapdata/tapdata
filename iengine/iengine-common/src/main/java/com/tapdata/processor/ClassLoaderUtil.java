package com.tapdata.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLClassLoader;
import java.util.Arrays;

public class ClassLoaderUtil {

  private static final Logger logger = LogManager.getLogger(ClassLoaderUtil.class);


  public static void recursivePrint(ClassLoader classLoader) {

    do {
      logger.info("class loader: {}", classLoader);
      if (classLoader instanceof URLClassLoader) {
        logger.info("url:  {}", Arrays.asList(((URLClassLoader) classLoader).getURLs()));
      }
      classLoader = classLoader != null ? classLoader.getParent() : null;
    } while (classLoader != null);

  }
}
