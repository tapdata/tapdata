package io.tapdata.sybase.cdc.service;

import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSV;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVOfBigFile;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.analyse.csv.CdcAccepter;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;

import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description ReadCsvFile create by Gavin
 * @create 2023/7/13 11:41
 **/
public class AnalyseCsvFile implements CdcStep<Void> {
    CdcRoot root;
    CdcPosition position;
    ReadCSV readCSV;
    CdcAccepter accepter;
    LinkedHashMap<String, TableTypeEntity> tapTable;

    public AnalyseCsvFile(CdcRoot root, CdcPosition position, ReadCSV readCSV) {
        this.root = root;
        this.position = position;
        this.readCSV = Optional.ofNullable(readCSV).orElse(new ReadCSVOfBigFile());
    }

    @Override
    public Void compile() {
        if (null == cdcFilePath) return null;
        readCSV.read(cdcFilePath, accepter);
        return null;
    }

    public Void compile(ReadCSV readCSV, int offset) {
        if (null == cdcFilePath) return null;
        readCSV.read(cdcFilePath, offset, accepter);
        return null;
    }

    public String cdcFilePath;

    public AnalyseCsvFile analyse(String cdcFilePath, LinkedHashMap<String, TableTypeEntity> tapTable, CdcAccepter accepter) {
        this.cdcFilePath = cdcFilePath;
        this.accepter = accepter;
        this.tapTable = tapTable;
        return this;
    }

    public CdcPosition getPosition() {
        return position;
    }
}
