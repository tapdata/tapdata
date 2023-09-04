package io.tapdata.sybase.cdc.dto.analyse.csv;

import io.tapdata.sybase.cdc.dto.analyse.csv.opencsv.SpecialField;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author GavinXiao
 * @description ReadCSV create by Gavin
 * @create 2023/7/13 11:43
 **/
public interface ReadCSV {
    public static final int CDC_BATCH_SIZE = 1000;

    public default void read(String csvPath, CdcAccepter accepter){
        read(csvPath, 0, accepter);
    }

    public void read(String csvPath, int offset, CdcAccepter accepter);

    public void read(String csvPath, int offset, List<SpecialField> specialFields, CdcAccepter accepter);
}
