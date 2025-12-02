package com.tapdata.tm.taskinspect.config;

import com.tapdata.tm.taskinspect.cons.TableFilterTypeEnum;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * 校验表过滤配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/12/2 09:47 Create
 */
@Setter
@Getter
public class TableFilter implements IConfig<TableFilter> {

    private TableFilterTypeEnum type;
    private List<String> tables;
    private String regex;

    @Override
    public TableFilter init(int depth) {
        setType(init(getType(), TableFilterTypeEnum.NONE));
        switch (getType()) {
            case INCLUDES, EXCLUDES -> setTables(init(getTables(), new ArrayList<>()));
            case INCLUDE_REGEX, EXCLUDE_REGEX -> {
                if (null == regex || regex.isBlank()) {
                    setRegex(".*");
                }
            }
        }
        return this;
    }
}
