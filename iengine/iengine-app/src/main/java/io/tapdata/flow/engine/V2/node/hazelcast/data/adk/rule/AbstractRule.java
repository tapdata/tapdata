package io.tapdata.flow.engine.V2.node.hazelcast.data.adk.rule;

import io.tapdata.flow.engine.V2.node.hazelcast.data.adk.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.adk.vo.increase.JudgeResult;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/30 09:31 Create
 * @description
 */
public abstract class AbstractRule {

    public void loadAdjustInfo(List<AdjustInfo> from, AdjustInfo to) {
        //do nothing
    }

    public abstract int sort();

    public abstract void check(AdjustInfo adjustInfo, JudgeResult result);
}
