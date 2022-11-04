package io.tapdata.bigquery.service.stage.tapvalue;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.logger.TapLogger;

public class TapDateSql implements TapValueForBigQuery{
    final static String DATA_FORMAT = "yyyy-MM-dd";
    @Override
    public String value(Object value) {
        String val = String.valueOf(value);
        try {
            return DateUtil.format(DateUtil.parseDate(val),DATA_FORMAT);
        }catch (Exception e){
            TapLogger.debug("","Can not format the date : {}",value);
            return "NULL";
        }
    }
}
