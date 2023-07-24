package io.tapdata.sybase.cdc.dto.analyse.csv;

import au.com.bytecode.opencsv.CSVReader;
import io.tapdata.entity.error.CoreException;
import io.tapdata.sybase.cdc.dto.read.ReadCSV;
import io.tapdata.sybase.cdc.dto.watch.CdcAccepter;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author GavinXiao
 * @description ReadCSVStageImpl create by Gavin
 * @create 2023/7/24 12:08
 **/
public class ReadCSVStageImpl implements ReadCSV {
    @Override
    public void read(String csvPath, CdcAccepter consumer) {
        List<List<String>> lines = new ArrayList<>();
        String[] strArr = null;
        CSVReader reader = null;
        try {
            reader = new CSVReader(new InputStreamReader(new FileInputStream(csvPath), StandardCharsets.UTF_8));
            while (null != (strArr = reader.readNext())) {
                lines.add(new ArrayList<>(Arrays.asList(strArr)));
                if (lines.size() >= CDC_BATCH_SIZE) {
                    consumer.accept(lines);
                    lines = new ArrayList<>();
                }
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle csv line, msg: " + e.getMessage());
        } finally {
            if (!lines.isEmpty()) {
                consumer.accept(lines);
                lines = null;
            }
            try {
                if (null != reader) {
                    reader.close();
                }
            } catch (Exception e) {
                throw new CoreException("Can not close CSV reader when monitor handle csv line, msg: " + e.getMessage());
            }
        }
    }
}
