package io.tapdata.connector.custom.constant;

public enum JSEngineEnum {

  NASHORN("nashorn"),
  GRAALVM_JS("graal.js"),
  ;

  private final String engineName;

  JSEngineEnum(String engineName) {
    this.engineName = engineName;
  }

  public String getEngineName() {
    return engineName;
  }

  public static JSEngineEnum getByEngineName(String engineName) {
    for (JSEngineEnum engineEnum : values()) {
      if (engineEnum.engineName.equals(engineName)) {
        return engineEnum;
      }
    }
    return NASHORN;
  }
}
