package io.tapdata.sybase.cdc.dto.watch;

import java.util.List;

/**
 * @author GavinXiao
 * @description CdcAccepter create by Gavin
 * @create 2023/7/21 16:05
 **/
public interface CdcAccepter {
    public void accept(List<List<String>> compileLines, int lastIndex);
}
