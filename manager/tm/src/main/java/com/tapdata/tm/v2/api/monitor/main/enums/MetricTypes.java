package com.tapdata.tm.v2.api.monitor.main.enums;

import lombok.Getter;


@Getter
public enum MetricTypes {
    API_SERVER(0),
    API(1),
    SERER(2),
    ALL(3);

    final int type;
    MetricTypes(int t) {
        this.type = t;
    }
}
