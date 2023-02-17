package com.tapdata.tm.worker;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/2/15 19:20 Create
 */
public class WorkerSingletonLock {

    private static final String FILE_PATH = "agent.singletonLock";

    private static String lockFilePath(String workDir) {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(workDir)) {
            sb.append(workDir).append("/").append(FILE_PATH);
        } else {
            sb.append(FILE_PATH);
        }
        return sb.toString();
    }

    private static String getSingletonLock(String filePath) {
        try (FileReader fr = new FileReader(filePath)) {
            StringBuilder buf = new StringBuilder();
            int c;
            while (-1 != (c = fr.read())) {
                buf.append((char)c);
            }
            return buf.toString();
        } catch (IOException e) {
            throw new RuntimeException("Load singleton lock failed: " + e.getMessage(), e);
        }
    }

    private static void setSingletonLock(String filePath, String str) {
        try (FileWriter fr = new FileWriter(filePath)) {
            fr.write(str);
        } catch (IOException e) {
            throw new RuntimeException("Store singleton lock failed: " + e.getMessage(), e);
        }
    }

    public static void check(String workDir, Function<String, String> call) {
        String filePath = lockFilePath(workDir);
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                File parentFile = file.getParentFile();
                if (!parentFile.exists()) {
                    parentFile.mkdirs();
                }
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new RuntimeException("Init singleton lock failed: " + e.getMessage(), e);
        }

        String singletonLock = getSingletonLock(filePath);
        singletonLock = call.apply(singletonLock);
        setSingletonLock(filePath, singletonLock);
    }

}
