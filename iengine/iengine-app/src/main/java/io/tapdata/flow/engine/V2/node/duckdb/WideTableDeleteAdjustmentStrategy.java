package io.tapdata.flow.engine.V2.node.duckdb;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface WideTableDeleteAdjustmentStrategy {
    boolean supports(WideTableDeleteAdjustmentContext context);

    List<Map<String, Object>> adjust(WideTableDeleteAdjustmentContext context) throws SQLException;
}
