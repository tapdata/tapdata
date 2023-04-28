package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connection.vo.Website;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;

import java.util.List;

/**
 * @author GavinXiao
 * @description TableWebsiteFunction create by Gavin
 * @create 2023/4/27 16:01
 **/
public interface TableWebsiteFunction extends TapConnectionFunction {
    Website getUrl(TapConnectionContext context, List<String> tables);
}
