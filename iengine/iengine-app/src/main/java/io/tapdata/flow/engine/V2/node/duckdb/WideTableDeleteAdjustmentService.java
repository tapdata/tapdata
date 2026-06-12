package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 删除语义调整服务，按策略链顺序处理子表删除场景。
 */
public class WideTableDeleteAdjustmentService {
    private static final Logger logger = LoggerFactory.getLogger(WideTableDeleteAdjustmentService.class);

    private final List<WideTableDeleteAdjustmentStrategy> strategies;

    public WideTableDeleteAdjustmentService(List<WideTableDeleteAdjustmentStrategy> strategies) {
        this.strategies = strategies == null ? Collections.emptyList() : strategies;
    }

    public List<Map<String, Object>> adjust(WideTableDeleteAdjustmentContext context) throws SQLException {
        if (context == null) {
            return Collections.emptyList();
        }
        for (WideTableDeleteAdjustmentStrategy strategy : strategies) {
            if (strategy != null && strategy.supports(context)) {
                logger.info("Adjusting delete semantics for source table {} with strategy {}",
                        context.getSourceTableName(), strategy.getClass().getSimpleName());
                return strategy.adjust(context);
            }
        }
        return context.getAfterResults();
    }
}
