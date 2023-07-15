package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.entity.schema.TapTable;

/**
 * @author GavinXiao
 * @description AnalyseRecord create by Gavin
 * @create 2023/7/14 9:59
 **/
public interface AnalyseRecord <V,T> {
    public T analyse(V record, TapTable tapTable);
}
