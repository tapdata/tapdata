package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.utils.DataMap;

public class TapFilter {
    protected DataMap match;

    public DataMap getMatch() {
        return match;
    }

    public void setMatch(DataMap match) {
        this.match = match;
    }
}
