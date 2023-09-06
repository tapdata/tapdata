package io.tapdata.sybase.cdc.dto.analyse.csv;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class NormalFileReader {
    public List<String> read(String csvPath, boolean negate) {
        List<String> lines = new ArrayList<>();
        try (
                FileInputStream inputStream = new FileInputStream(csvPath);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine())) {
                if ("".equals(line.trim())) continue;
                lines.add(line.length() > 300 ? (line.substring(0, 300) + "...") : line);
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle csv line, msg: " + e.getMessage());
        }
        if (negate && lines.size() > 1) {
            Collections.reverse(lines);
        }
        return lines;
    }

    public static final int MAX_READ_LINE = 50;
    public String readString(String csvPath, boolean negate) {
        List<String> read = read(csvPath, negate);
        StringJoiner builder = new StringJoiner("\n");
        int lines = 0;
        if (null != read && !read.isEmpty()) {
            for (String s : read) {
                builder.add(s);
                lines++;
                if (lines >= MAX_READ_LINE) break;
            }
        } else {
            builder.add("-");
        }
        return builder.toString();
    }

    private Log log;

    public void setLog(Log log) {
        this.log = log;
    }
}
