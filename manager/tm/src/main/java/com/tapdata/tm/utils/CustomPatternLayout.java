package com.tapdata.tm.utils;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.Map;

public class CustomPatternLayout extends PatternLayout {
    private Map<String, Object> oemConfigmap = OEMReplaceUtil.getOEMConfigMap("log/replace.json");

    @Override
    public String doLayout(ILoggingEvent event) {
        String formatMessage = super.doLayout(event);
        if (MapUtils.isNotEmpty(oemConfigmap)) {
            return OEMReplaceUtil.replace(formatMessage, oemConfigmap);
        }
        return formatMessage;
    }

    public Map<String, Object> getOemConfigmap() {
        return oemConfigmap;
    }

    public void setOemConfigmap(Map<String, Object> oemConfigmap) {
        this.oemConfigmap = oemConfigmap;
    }
}
