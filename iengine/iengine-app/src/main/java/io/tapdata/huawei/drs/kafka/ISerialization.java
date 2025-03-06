package io.tapdata.huawei.drs.kafka;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.huawei.drs.kafka.StoreType;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;

import java.util.function.BiConsumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/21 17:24 Create
 */
public interface ISerialization {
    boolean isDeduceSchema();

    IType getType(String type);

    void process(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer, HazelcastProcessorBaseNode.ProcessResult processResult);

    static ISerialization create(String storeType, String fromDBType, boolean isDeduceSchema) {
        StoreType storeTypeEnum = StoreType.fromValue(storeType);
        switch (storeTypeEnum) {
            case JSON:
                return JsonSerialization.create(fromDBType, isDeduceSchema);
            case AVRO:
            case JSON_C:
            default:
                throw new RuntimeException("un-support storeType '" + storeType + "'");
        }
    }
}
