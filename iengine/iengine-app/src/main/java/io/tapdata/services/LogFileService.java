package io.tapdata.services;

import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.observable.logging.appender.FileAppender;
import io.tapdata.observable.metric.py.ZipUtils;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/26 15:04
 */
@RemoteService
@Slf4j
public class LogFileService {

    public List<Map<String, Object>> describeLogFiles(String taskId) {
        return FileUtils.listFiles(new File(FileAppender.LOG_PATH), null, true)
                .stream()
                .filter(f -> StringUtils.isBlank(taskId) || f.getName().contains(taskId))
                .map(f -> {
                    BasicFileAttributes fileAttributes = null;
                    try {
                        fileAttributes = Files.readAttributes(Paths.get(f.getAbsolutePath()), BasicFileAttributes.class);
                    } catch (IOException e) {
                        log.error("Read file attributes failed {}", f.getAbsolutePath(), e);
                    }
                    Map<String, Object> fileMeta = new HashMap<>();
                    fileMeta.put("filename", f.getName());
                    fileMeta.put("size", f.length());
                    fileMeta.put("lastModified", f.lastModified());
                    if (fileAttributes != null) {
                        if (fileAttributes.creationTime() != null)
                            fileMeta.put("creationTime", fileAttributes.creationTime().toMillis());
                        if (fileAttributes.lastAccessTime() != null)
                            fileMeta.put("lastAccessTime", fileAttributes.lastAccessTime().toMillis());
                    }
                    return fileMeta;
                }).collect(Collectors.toList());
    }

    public String deleteLogFile(String fileName) {
        try {
            File logDirector = FileUtils.getFile(FileAppender.LOG_PATH);
            File file = FileUtils.getFile(FileAppender.LOG_PATH, fileName);
            if (!logDirector.getAbsolutePath().equals(file.getParentFile().getAbsolutePath())) {
                return "Only delete log file in logs directory";
            }
            if (!file.exists()) {
                return "Not exists";
            }
            if (!file.isFile()) {
                return "Not is file";
            }
            return file.delete() ? "ok" : "failed";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    public FileMeta downloadFile(String filename) {
        File file = new File(FileAppender.LOG_PATH, filename);

        if (file.exists()) {
            try {
                if (filename.endsWith(".log")) {
                    File zipFile = new File(file.getParentFile(), file.getName() + ".zip");
                    Files.deleteIfExists(zipFile.toPath());
                    OutputStream output = Files.newOutputStream(zipFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    ZipUtils.zip(file, output);
                    IOUtils.closeQuietly(output);

                    file = zipFile;
                }

                if (file.exists())
                    return FileMeta.builder().filename(file.getName()).fileSize(file.length()).code("ok")
                        .fileInputStream(Files.newInputStream(file.toPath(), StandardOpenOption.READ))
                        .transferFile(true).build();
                else
                    return FileMeta.builder().filename(file.getName()).code("File not exists.")
                            .transferFile(false).build();
            } catch (Exception e) {
                log.error("Read file failed", e);
                return FileMeta.builder().filename(filename).code("ReadFileError").build();
            }
        }
        return FileMeta.builder().filename(filename).code("FileNotFound").build();
    }
}
