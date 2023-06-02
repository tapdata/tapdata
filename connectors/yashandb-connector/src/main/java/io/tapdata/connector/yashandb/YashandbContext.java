package io.tapdata.connector.yashandb;

import io.tapdata.connector.yashandb.config.YashandbConfig;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Author:Skeet
 * Date: 2023/5/16
 **/

@Setter
@Getter
public class YashandbContext implements AutoCloseable {
    private static final String TAG = YashandbContext.class.getSimpleName();
    private TapConnectionContext tapConnectionContext;
    private YashandbConfig yashandbConfig;
    private Connection connection;
    private Statement statement;
    public TapConnectionContext tapConnectionContext(){
        return tapConnectionContext;
    }
    @Override
    public void close() throws Exception {

    }
}
