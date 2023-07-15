package io.tapdata.sybase.cdc.dto.read;

import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.List;

/**
 * @author GavinXiao
 * @description ReadCSV create by Gavin
 * @create 2023/7/13 11:43
 **/
public interface ReadCSV {
    public List<List<String>> read(String csvPath);
}
