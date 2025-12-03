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
 * @version v1.0 2025/11/30 09:44 Create
 * @description 2) If any of the hazelcast queues fill up to over 95% of their limit,
 * then we can consider to lower the batch size
 */
@Component
public class QueueFillUpLimitRule extends AbstractRule implements InitializingBean {

    @Override
    public int sort() {
        return 10;
    }

    @Override
    public void check(AdjustInfo adjustInfo, JudgeResult result) {
        double rateOf = result.getRate();
        String taskId = adjustInfo.getTaskId();
        double rate = 1.0D * adjustInfo.getEventQueueSize() / adjustInfo.getEventQueueCapacity();
        if (adjustInfo.getBatchSize() > 1 && rate > adjustInfo.getEventQueueFullThreshold()) {
            result.setHasJudge(true);
            //Isn't the delay exceeding the threshold here, and the capacity has not reached 70%, such as 20%,
            // with a default queue limit of 95%?
            // At this point, available=95% -20%=75%, indicating that it can only increase by another 75% at most.
            rateOf = rateOf - (rate - adjustInfo.getEventQueueFullThreshold());
            result.setType(-1);
            double r = rateOf;
            String msg = String.format("Judgment Rule 2 - [%s], rate of %.2f , judge info: %s, %d", taskId, r, JSON.toJSONString(adjustInfo), System.currentTimeMillis());
            AdjustBatchSizeFactory.debug(taskId, msg);
            result.setDetail(msg);
            result.setReason(String.format("Increase event queue is full, threshold: %.2f%%, capacity used: %.2f%%", adjustInfo.getEventQueueFullThreshold() * 100, rate * 100D));
            result.setRate(rateOf);
        }
    }

    @Override
    public void afterPropertiesSet() {
        IncreaseRuleFactory.factory().register(this);
    }
}
