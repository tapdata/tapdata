package io.tapdata.bigquery.service.stage.tapvalue;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.logger.TapLogger;

public class TapDateTime implements TapValueForBigQuery{
    public final String DATA_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    @Override
    public String value(Object value) {
        String val = String.valueOf(value);
        try {
            return DateUtil.format(DateUtil.parseDate(val),DATA_TIME_FORMAT);
        }catch (Exception e){
            TapLogger.debug("","Can not format the date time : {}",value);
            return "NULL";
        }
    }
}
