package io.tapdata.connector.gauss;

import io.tapdata.common.CommonSqlMaker;

/**
 * Author:Skeet
 * Date: 2023/6/5
 **/
public class GaussSqlMaker extends CommonSqlMaker {
    private Boolean closeNotNull;

    public GaussSqlMaker closeNotNull(Boolean closeNotNull) {
        this.closeNotNull = closeNotNull;
        return this;
    }
}
