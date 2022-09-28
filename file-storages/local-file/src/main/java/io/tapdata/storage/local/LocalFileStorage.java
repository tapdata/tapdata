package io.tapdata.storage.local;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class LocalFileStorage implements TapFileStorage {

    private final static String TAG = LocalFileStorage.class.getSimpleName();
    private Map<String, Object> params;

    @Override
    public void init(Map<String, Object> params) {
        this.params = params;
    }

    @Override
    public void destroy() {

    }

    @Override
    public TapFile getFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        } else {
            TapFile tapFile = new TapFile();
            tapFile.type(file.isFile() ? TapFile.TYPE_FILE : TapFile.TYPE_DIRECTORY)
                    .name(file.getName())
                    .path(file.getAbsolutePath())
                    .length(file.length())
                    .lastModified(file.lastModified());
            return tapFile;
        }
    }

    @Override
    public InputStream readFile(String path) {
        try {
            return Files.newInputStream(Paths.get(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFileExist(String path) {
        File file = new File(path);
        return file.exists() && file.isFile();
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        try {
            Files.move(Paths.get(sourcePath), Paths.get(destPath));
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean delete(String path) {
        return false;
    }

    @Override
    public TapFile saveFile(String path, InputStream is, boolean canReplace) {
        return null;
    }

    @Override
    public void getFilesInDirectory(String directoryPath, String includeReg, String excludeReg, boolean recursive, int batchSize, Consumer<List<TapFile>> consumer) {

    }

    @Override
    public boolean isDirectoryExist(String path) {
        return false;
    }

}
