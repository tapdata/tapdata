package io.tapdata.sybase.cdc.dto.analyse.csv;

import java.util.List;

/**
 * @author GavinXiao
 * @description CdcAccepter create by Gavin
 * @create 2023/7/21 16:05
 **/
public interface CdcAccepter {
    public void accept(List<String[]> compileLines, int firstIndex, int lastIndex);
}
