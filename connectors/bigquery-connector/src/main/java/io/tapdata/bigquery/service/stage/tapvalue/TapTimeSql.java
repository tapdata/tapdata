package io.tapdata.bigquery.service.stage.tapvalue;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.logger.TapLogger;

public class TapTimeSql implements TapValueForBigQuery{
    final static String TIME_FORMAT = "HH:mm:ss.SSSSSS";
    @Override
    public String value(Object value) {
        String val = String.valueOf(value);
        try {
            return DateUtil.format(DateUtil.parseDate(val),TIME_FORMAT);
        }catch (Exception e){
            TapLogger.debug("","Can not format the time : {}",value);
            return "NULL";
        }
    }
}
