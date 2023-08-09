package io.tapdata.sybase.cdc.service;

import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.csv.CdcAccepter;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSV;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVOfBigFile;
import io.tapdata.sybase.cdc.dto.analyse.csv.opencsv.SpecialField;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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
    List<SpecialField> specialFields;

    public AnalyseCsvFile(CdcRoot root, CdcPosition position, ReadCSV readCSV) {
        this.root = root;
        this.position = position;
        ReadCSVOfBigFile readCSVOfBigFile = new ReadCSVOfBigFile();
        readCSVOfBigFile.setLog(root.getContext().getLog());
        this.readCSV = Optional.ofNullable(readCSV).orElse(readCSVOfBigFile);
    }

    @Override
    public Void compile() {
        if (null == cdcFilePath) return null;
        readCSV.read(cdcFilePath, accepter);
        return null;
    }

    public Void compile(ReadCSV readCSV, int offset) {
        if (null == cdcFilePath) return null;
        //readCSV.read(cdcFilePath, accepter);
        readCSV.read(cdcFilePath, offset, accepter);
        //readCSV.read(cdcFilePath, offset, specialFields, accepter);
        return null;
    }

    public String cdcFilePath;

    public AnalyseCsvFile analyse(String cdcFilePath, LinkedHashMap<String, TableTypeEntity> tapTable, CdcAccepter accepter) {
        this.cdcFilePath = cdcFilePath;
        this.accepter = accepter;
        this.tapTable = tapTable;
        //specialFields();
        return this;
    }

    public CdcPosition getPosition() {
        return position;
    }

    public void specialFields() {
        if (null != tapTable && !tapTable.isEmpty()) {
            specialFields = new ArrayList<>();
            AtomicInteger index = new AtomicInteger(0);
            tapTable.forEach((name, entity) -> {
                SpecialField specialField = new SpecialField();
                int length = entity.getLength();
                String type = entity.getType();
                //@todo IMAGE BINARY TEXT
                specialField.setNeedSpecial(type.startsWith("TEXT")
                        || type.startsWith("BINARY")
                        || type.startsWith("IMAGE")
                );
                specialField.setLength(length);
                specialField.setIndex(index.incrementAndGet());
                specialField.setType(type);
                specialFields.add(specialField);
            });
        }
    }
}
