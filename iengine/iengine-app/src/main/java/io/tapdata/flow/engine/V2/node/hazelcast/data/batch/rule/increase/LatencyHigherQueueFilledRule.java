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
 * @version v1.0 2025/11/30 09:46 Create
 * @description
 * 3) If the data latency is higher than a threshold (e.g., 1 second),
 *      and none of the hazelcast queues is filled above 70%,
 *      then consider to increase the batch size
 */
@Component
public class LatencyHigherQueueFilledRule extends AbstractRule implements InitializingBean {

    @Override
    public int sort() {
        return 20;
    }

    @Override
    public void check(AdjustInfo adjustInfo, JudgeResult result) {
        double rateOf = result.getRate();
        String taskId = adjustInfo.getTaskId();
        double rate = 1.0D * adjustInfo.getEventQueueSize() / adjustInfo.getEventQueueCapacity();
        if (adjustInfo.getEventDelay() > adjustInfo.getEventDelayThresholdMs() && rate < adjustInfo.getEventQueueIdleThreshold()) {
            result.setHasJudge(true);
            double available = adjustInfo.getEventQueueFullThreshold() - rate;
            if (adjustInfo.getEventDelay() < 1.5D * adjustInfo.getEventDelayThresholdMs() && Math.abs(available) < Math.abs(rateOf)) {
                result.setRate(rateOf);
                String msg = String.format("Judgment Rule 3 - [%s] available less than rateOf, rate of %.2f , judge info: %s, %d", taskId, rateOf, JSON.toJSONString(adjustInfo), System.currentTimeMillis());
                AdjustBatchSizeFactory.debug(taskId, msg);
                result.setDetail(msg);
                result.setReason(String.format("Increase event delay(%dms) is higher than the threshold(%dms), but the event queue is not full (available only: %.2f)", adjustInfo.getEventDelay(), adjustInfo.getEventDelayThresholdMs(), available * 100D));
                result.setCompleted(true);
                return;
            }
            double blockUpRate = -2D * (adjustInfo.getEventQueueIdleThreshold() - rate) / 5D;
            if (result.getType() != 0) {
                available = (available * 2D / 5D);
            } else {
                double downRate = -.8D * Math.max(0.8D, 1D * (adjustInfo.getEventDelay() - adjustInfo.getEventDelayThresholdMs()) / adjustInfo.getEventDelay());
                rateOf += downRate;
            }
            rateOf = rateOf + available + blockUpRate;
            result.setType(1);
            String msg = String.format("Judgment Rule 3 - [%s], rate of %.2f, up rate: %.2f, judge info: %s, %d", taskId, rateOf, available, JSON.toJSONString(adjustInfo), System.currentTimeMillis());
            AdjustBatchSizeFactory.debug(taskId, msg);
            result.setReason(String.format("Increase event delay(%dms) is higher than the threshold(%dms), but the event queue is not full (only used: %.2f%%)", adjustInfo.getEventDelay(), adjustInfo.getEventDelayThresholdMs(), rate * 100D));
            result.setDetail(msg);
            result.setRate(rateOf);
            return;
        }
        if (adjustInfo.getEventDelay() > adjustInfo.getEventDelayThresholdMs() * 1.5D && rate >= adjustInfo.getEventQueueIdleThreshold()) {
            result.setHasJudge(true);
            double available = (adjustInfo.getEventQueueSize() / (adjustInfo.getEventQueueIdleThreshold() * adjustInfo.getEventQueueCapacity()))  - 1D;
            rateOf += (available * 2D / 3D);
            String msg = String.format("Judgment Rule 3 - [%s], rate of %.2f, available to up rate: %.2f, judge info: %s, %d: source cdc has long delay", taskId, rateOf, available, JSON.toJSONString(adjustInfo), System.currentTimeMillis());
            result.setReason(String.format("Increase event delay(%dms) is higher than the 1.5 * threshold(%dms), also the event queue is full(used: %.2f%%, available to up rate: %.2f%%), Batch size change rate: %.2f%%", adjustInfo.getEventDelay(), adjustInfo.getEventDelayThresholdMs(), rate * 100D, available * 100D, rateOf * 100D));
            result.setDetail(msg);
            result.setRate(rateOf);
        }
    }

    @Override
    public void afterPropertiesSet() {
        IncreaseRuleFactory.factory().register(this);
    }
}
