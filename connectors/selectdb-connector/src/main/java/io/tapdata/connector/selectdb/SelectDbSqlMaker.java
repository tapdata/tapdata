package io.tapdata.connector.selectdb;

import io.tapdata.common.CommonSqlMaker;

/**
 * Author:Skeet
 * Date: 2023/6/2
 **/
public class SelectDbSqlMaker extends CommonSqlMaker {
    private Boolean closeNotNull;

    public SelectDbSqlMaker(char escapeChar) {
        super(escapeChar);
    }

    public SelectDbSqlMaker closeNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
        return this;
    }




}
