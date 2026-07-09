package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.flow.engine.V2.node.hazelcast.dynamic.FunctionProxy;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 11:11 Create
 * @description
 */
public class StreamReadFunctionProxy extends StreamReadNormalBaseProxy<StreamReadFunction> implements StreamReadFunction {

    public static StreamReadFunctionProxy instance(StreamReadFunction function) {
        if (null == function) {
            return null;
        }
        return new StreamReadFunctionProxy(function);
    }

    public StreamReadFunctionProxy(StreamReadFunction function) {
        super(function);
    }

    @Override
    public void streamRead(TapConnectorContext context, List<String> list, Object o, int batchSize, StreamReadConsumer streamReadOneByOneConsumer) throws Throwable {
        boolean doNext = doBefore(new Object[]{context, list, o, streamReadOneByOneConsumer});
        if (!doNext) {
            return;
        }
        try {
            getFunction().streamRead(context, list, o, batchSize, streamReadOneByOneConsumer);
        } finally {
            doAfter(new Object[]{context, list, o, streamReadOneByOneConsumer});
        }
    }
}
