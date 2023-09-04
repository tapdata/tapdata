package io.tapdata.sybase.cdc.dto.analyse.csv;

import io.tapdata.sybase.cdc.dto.analyse.csv.opencsv.SpecialField;

import java.util.List;

/**
 * @author GavinXiao
 * @description ReadCSV create by Gavin
 * @create 2023/7/13 11:43
 **/
public interface ReadCSV {
    public static final int CDC_BATCH_SIZE = 500;
    public static final int MAX_LINE_EVERY_CSV_FILE = 1000;
    public static final int DEFAULT_CACHE_TIME_OF_CSV_FILE = 10;//min

    public default void read(String csvPath, CdcAccepter accepter) {
        read(csvPath, 0, accepter);
    }

    public void read(String csvPath, int offset, CdcAccepter accepter);

    public void read(String csvPath, int offset, List<SpecialField> specialFields, CdcAccepter accepter);

    public default int getCdcBatchSize() {
        return CDC_BATCH_SIZE;
    }

    public default void setCdcBatchSize(int size) {
    }
}
