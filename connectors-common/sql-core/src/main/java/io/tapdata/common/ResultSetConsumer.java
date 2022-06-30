package io.tapdata.common;

import java.sql.ResultSet;

/**
 * @author samuel
 * @date 2022-04-28 17:24
 **/
public interface ResultSetConsumer {
    void accept(ResultSet rs) throws Throwable;
}
