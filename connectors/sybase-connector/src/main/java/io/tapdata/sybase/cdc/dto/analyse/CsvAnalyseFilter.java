package io.tapdata.sybase.cdc.dto.analyse;

import java.util.Map;

/**
 * @author GavinXiao
 * @description CsvAnalyseFilter create by Gavin
 * @create 2023/8/9 15:22
 **/
public interface CsvAnalyseFilter {
    boolean filter(Map<String, Object> before, Map<String, Object> after, Map<String, Object> cdcInfo);
}