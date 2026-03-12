package com.tapdata.tm.skiperrortable.vo;

import com.tapdata.tm.taskinspect.vo.MapCreator;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 跳过错误表-恢复（删除错误记录）
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/10 17:50 Create
 */
@Getter
@Setter
public class SkipErrorTableRecoveredVo implements Serializable {

    public static final String FIELD_SOURCE_TABLES = "sourceTables";

    private List<String> sourceTables;

    public SkipErrorTableRecoveredVo sourceTables(String... sourceTables) {
        setSourceTables(List.of(sourceTables));
        return this;
    }

    public Map<String, Object> toMap() {
        return MapCreator.create(FIELD_SOURCE_TABLES, getSourceTables());
    }

    public static SkipErrorTableRecoveredVo create(String... sourceTables) {
        return new SkipErrorTableRecoveredVo().sourceTables(sourceTables);
    }
}
