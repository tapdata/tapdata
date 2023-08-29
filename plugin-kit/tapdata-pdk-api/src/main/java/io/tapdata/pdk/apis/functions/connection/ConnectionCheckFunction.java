package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectionFunction;

import java.util.List;
import java.util.function.Consumer;

public interface ConnectionCheckFunction extends TapConnectionFunction {
    /**
     * Connection check on the datasource.
     *
     * @param connectionContext
     * @param items items need to check, maybe not all the items but some of them. if null, mean all items need be checked.
     * @param consumer
     * @throws Throwable
     */
    void check(TapConnectionContext connectionContext, List<String> items, Consumer<ConnectionCheckItem> consumer) throws Throwable;
}
