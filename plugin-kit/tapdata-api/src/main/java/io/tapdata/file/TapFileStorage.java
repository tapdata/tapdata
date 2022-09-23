package io.tapdata.file;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface TapFileStorage {
    void init(Map<String, Object> params);
    void destroy();
    TapFile getFile(String path);
    InputStream readFile(String path);
    boolean isFileExist(String path);
    boolean move(String sourcePath, String destPath);
    boolean delete(String path);
    TapFile saveFile(String path, InputStream is, boolean canReplace);
    List<TapFile> getFilesInDirectory(String directoryPath, List<String> matchingReg, boolean recursive);
    boolean isDirectoryExist(String path);
}