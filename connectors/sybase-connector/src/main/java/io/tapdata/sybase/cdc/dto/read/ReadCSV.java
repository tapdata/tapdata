package io.tapdata.sybase.cdc.dto.read;

import io.tapdata.sybase.cdc.dto.watch.CdcAccepter;

/**
 * @author GavinXiao
 * @description ReadCSV create by Gavin
 * @create 2023/7/13 11:43
 **/
public interface ReadCSV {
    public static final int CDC_BATCH_SIZE = 200;

    public void read(String csvPath, CdcAccepter accepter);
}
