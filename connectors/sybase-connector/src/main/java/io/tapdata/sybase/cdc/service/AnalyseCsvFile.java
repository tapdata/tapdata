package io.tapdata.sybase.cdc.service;

import au.com.bytecode.opencsv.CSVReader;
import io.tapdata.entity.error.CoreException;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.read.ReadCSV;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description ReadCsvFile create by Gavin
 * @create 2023/7/13 11:41
 **/
public class AnalyseCsvFile implements CdcStep<List<List<String>>> {
    CdcRoot root;
    CdcPosition position;
    ReadCSV readCSV;

    public AnalyseCsvFile(CdcRoot root, CdcPosition position, ReadCSV readCSV) {
        this.root = root;
        this.position = position;
        this.readCSV = Optional.ofNullable(readCSV).orElse(DEFAULT_READ_CSV);
    }

    @Override
    public List<List<String>> compile() {
        if (null == line) return null;
        return readCSV.read(line);
    }

    public String line;

    public AnalyseCsvFile analyse(String line) {
        this.line = line;
        return this;
    }

    public static final ReadCSV DEFAULT_READ_CSV = csvPath -> {
        List<List<String>> lines = new ArrayList<>();
        String[] strArr = null;
        CSVReader reader = null;
        try {
            reader = new CSVReader(new InputStreamReader(new FileInputStream(csvPath), StandardCharsets.UTF_8));
            while (null != (strArr = reader.readNext())) {
                //System.out.println(strArr[0] + "---" + strArr[1] + "----" + strArr[2]);
                lines.add(new ArrayList<>(Arrays.asList(strArr)));
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle csv line, msg: " + e.getMessage());
        } finally {
            try {
                if (null != reader) {
                    reader.close();
                }
            } catch (Exception e) {
                throw new CoreException("Can not close CSV reader when monitor handle csv line, msg: " + e.getMessage());
            }
        }
        return lines;
    };

    public CdcPosition getPosition() {
        return position;
    }
}
