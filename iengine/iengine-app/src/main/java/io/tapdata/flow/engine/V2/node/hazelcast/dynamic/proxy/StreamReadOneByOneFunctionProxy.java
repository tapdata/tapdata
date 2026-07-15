package io.tapdata.flow.engine.V2.node.hazelcast.dynamic.proxy;

import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadOneByOneFunction;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/9 10:45 Create
 * @description
 */
public class StreamReadOneByOneFunctionProxy extends StreamReadNormalBaseProxy<StreamReadOneByOneFunction> implements StreamReadOneByOneFunction {

    public StreamReadOneByOneFunctionProxy(StreamReadOneByOneFunction function) {
        super(function);
    }

    public static StreamReadOneByOneFunctionProxy instance(StreamReadOneByOneFunction function) {
        if (null == function) {
            return null;
        }
        return new StreamReadOneByOneFunctionProxy(function);
    }

    @Override
    public void streamRead(TapConnectorContext context, List<String> list, Object o, StreamReadOneByOneConsumer consumer) throws Throwable {
        boolean doNext = doBefore(new Object[]{context, list, o, consumer});
        if (!doNext) {
            return;
        }
        try {
            getFunction().streamRead(context, list, o, consumer);
        } finally {
            doAfter(new Object[]{context, list, o, consumer});
        }
    }
}
