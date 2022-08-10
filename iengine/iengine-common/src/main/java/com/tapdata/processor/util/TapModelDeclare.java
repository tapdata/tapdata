package com.tapdata.processor.util;

import io.tapdata.entity.schema.TapTable;

public class TapModelDeclare {

  private TapModelDeclare() {

  }


  public static void addField(TapTable tapTable, String fieldName, String tapType, String dataType) {

  }

  public static void removeField(TapTable tapTable, String fieldName) {
    tapTable.getNameFieldMap().remove(fieldName);
  }
}
