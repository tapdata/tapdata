package io.tapdata.zoho.service.zoho.schemaLoader;

import cn.hutool.core.date.DateUtil;
import io.tapdata.zoho.ZoHoConnector;

public class Schema {
    ZoHoConnector zoHoConnector;

    public synchronized boolean isAlive() {
        return null != zoHoConnector && this.zoHoConnector.isAlive();
    }

    public void init(ZoHoConnector zoHoConnector) {
        this.zoHoConnector = zoHoConnector;
    }

    public void out() {
        zoHoConnector = null;
    }

    public Long parseZoHoDatetime(String referenceTimeStr){
        return DateUtil.parse(
                referenceTimeStr.replaceAll("Z", "").replaceAll("T", " "),
                "yyyy-MM-dd HH:mm:ss.SSS").getTime();
    }
}
