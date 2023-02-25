package io.tapdata.bigquery.service.stage.tapvalue;

import cn.hutool.core.codec.Base64;

public class TapBinarySql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return " FROM_BASE64('"+ Base64.encode(String.valueOf(value)) +"') ";
    }
}

