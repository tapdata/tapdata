package com.tapdata.tm.license.util;

public class SidGenerator {

  /**
   * java -cp tm-0.0.1.jar -Dloader.main=com.tapdata.tm.license.util.SidGenerator org.springframework.boot.loader.PropertiesLauncher
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.out.println("SID: "+SidUtil.generatorSID());
  }
}
