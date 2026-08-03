package com.tapdata.tm.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.exception.TapExLevel;
import io.tapdata.exception.TapExType;

@TapExClass(code = 42, module = "No Primary Key Virtual Field", prefix = "NPKVF", describe = "No Primary Key Virtual Field")
public interface NoPrimaryKeyVirtualFieldExCode_42 {

    @TapExCode(
            describe = "Fail to generate HashKey for DML event of source table without primary key",
            describeCN = "源端无主键表DML事件构建HashKey失败",
            dynamicDescription = "Fail to generate HashKey for DML event of source table without primary key, error event: {}",
            dynamicDescriptionCN = "源端无主键表DML事件构建HashKey失败，错误事件：{}",
            type = TapExType.RUNTIME,
            level = TapExLevel.CRITICAL
    )
    String GENERATE_HASH_KEY_FAILED = "42001";

    @TapExCode(
            describe = "Fail to find MD5 algorithm",
            describeCN = "找不到MD5算法",
            type = TapExType.RUNTIME,
            level = TapExLevel.CRITICAL
    )
    String MD5_ALGORITHM_NOT_FOUND = "42002";

    @TapExCode(
            describe = "The fields used to build the HashKey of the source table are incomplete",
            describeCN = "源端构建Hash的字段不完整",
            dynamicDescription = "The fields used to build the HashKey of the source table are incomplete, missing fields: {}",
            dynamicDescriptionCN = "源端构建Hash的字段不完整，缺失字段：{}",
            type = TapExType.RUNTIME,
            level = TapExLevel.CRITICAL
    )
    String HASH_KEYS_INCOMPLETE = "42003";
}
