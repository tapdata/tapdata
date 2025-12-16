package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase;

import com.alibaba.fastjson.JSON;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.AdjustBatchSizeFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.AbstractRule;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.IncreaseRuleFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/30 09:43 Create
 * @description
 *
 * 1) If the batch returned is smaller than the batch size,
 *      that means some kind of time-out occurred and we can consider to lower the batch size
 */
@Component
public class SizeMoreEventSizeRule extends AbstractRule implements InitializingBean {

    @Override
    public int sort() {
        return 0;
    }

    @Override
    public void check(AdjustInfo adjustInfo, JudgeResult result) {
        String taskId = adjustInfo.getTaskId();
        if (adjustInfo.getBatchSize() > adjustInfo.getEventSize()) {
            result.setHasJudge(true);
            result.setType(-2);
            double rateOf = -1d * (adjustInfo.getBatchSize() - adjustInfo.getEventSize()) / adjustInfo.getBatchSize();
            double r = rateOf;
            String msg = String.format("Judgment Rule 1 - [%s], rate of %.2f , judge info: %s, %d", taskId, r, JSON.toJSONString(adjustInfo), System.currentTimeMillis());
            AdjustBatchSizeFactory.debug(taskId, msg);
            result.setDetail(msg);
            result.setReason("Increase read size is larger than the number of events");
            result.setRate(rateOf);
        }
    }

    @Override
    public void afterPropertiesSet() {
        IncreaseRuleFactory.factory().register(this);
    }
}
