package io.tapdata.flow.engine.V2.node.hazelcast.data.adk.rule;

import io.tapdata.flow.engine.V2.node.hazelcast.data.adk.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.adk.vo.increase.JudgeResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/11/30 10:57 Create
 * @description
 */
public final class IncreaseRuleFactory {
    static volatile IncreaseRuleFactory FACTORY;
    final List<AbstractRule> rules = new ArrayList<>(8);

    public IncreaseRuleFactory() {

    }

    public static IncreaseRuleFactory factory() {
        if (FACTORY == null) {
            synchronized (IncreaseRuleFactory.class) {
                if (FACTORY == null) {
                    FACTORY = new IncreaseRuleFactory();
                }
            }
        }
        return FACTORY;
    }

    public <T extends AbstractRule>void register(T rule) {
        if (null == rule) {
            return;
        }
        if (rules.contains(rule)) {
            return;
        }
        rules.add(rule);
        rules.sort(Comparator.comparing(AbstractRule::sort));
    }

    public void loadOneByOne(List<AdjustInfo> adjustInfos, AdjustInfo adjustInfo) {
        for (AbstractRule rule : this.rules) {
            rule.loadAdjustInfo(adjustInfos, adjustInfo);
        }
    }


    public void each(AdjustInfo adjustInfo, JudgeResult result) {
        for (AbstractRule rule : this.rules) {
            rule.check(adjustInfo, result);
            if (result.isCompleted()) {
                return;
            }
        }
    }
}
