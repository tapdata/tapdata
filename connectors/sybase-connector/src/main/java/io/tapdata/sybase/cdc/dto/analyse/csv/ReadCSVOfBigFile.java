package io.tapdata.sybase.cdc.dto.analyse.csv;

import io.tapdata.entity.error.CoreException;
import io.tapdata.sybase.cdc.dto.read.ReadCSV;
import io.tapdata.sybase.cdc.dto.watch.CdcAccepter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * @author GavinXiao
 * @description ReadCSVOfBigFile create by Gavin
 * @create 2023/7/24 12:10
 **/
public class ReadCSVOfBigFile implements ReadCSV {
    @Override
    public void read(String csvPath, CdcAccepter consumer) {
        List<List<String>> lines = new ArrayList<>();
        String[] strArr = null;
        String csvLine = "";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(new File(csvPath)), StandardCharsets.UTF_8));
            //reader = new CSVReader(new InputStreamReader(new FileInputStream(csvPath), StandardCharsets.UTF_8));
            while (null != (csvLine = reader.readLine())) {
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

    public void read(String filePath) {
        File file = new File(filePath);
        BufferedReader buf = null;
        try {
            buf = new BufferedReader(new InputStreamReader(
                    new FileInputStream(file), StandardCharsets.UTF_8));
            StringJoiner tempString = new StringJoiner("\n");
            String temp = null;
            int count = 0;
            while ((temp = buf.readLine()) != null) {
                tempString.add(temp);
                count++;
                if (count >= CDC_BATCH_SIZE) {

                }
            }
        } catch (Exception e) {
            e.getStackTrace();
        } finally {
            if (buf != null) {
                try {
                    buf.close();
                } catch (IOException e) {
                    e.getStackTrace();
                }
            }
        }
    }
}
