package io.tapdata.pdk.core.constants;

import io.tapdata.entity.error.UnknownCodecException;

/**
 * 数据源质量控制类型
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/10 14:21 Create
 */
public enum DataSourceQCType {
    Alpha, // 通过TDD基础测试
    Beta, // 通过TDD集成测试
    GA, // 通过公司QA测试
    ;

    public static DataSourceQCType parse(String str) {
        if (null == str || (str = str.trim()).isEmpty()) {
            return null;
        }
        for (DataSourceQCType type : values()) {
            if (type.name().equalsIgnoreCase(str)) {
                return type;
            }
        }
        throw new UnknownCodecException("Unknown DataSourceQCType of '" + str + "'");
    }
}
