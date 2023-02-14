package io.tapdata.storage.local;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.FileMatchKit;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class LocalFileStorage implements TapFileStorage {

    @Override
    public void init(Map<String, Object> params) {

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
            return toTapFile(file);
        }
    }

    private TapFile toTapFile(File file) {
        TapFile tapFile = new TapFile();
        tapFile.type(file.isFile() ? TapFile.TYPE_FILE : TapFile.TYPE_DIRECTORY)
                .name(file.getName())
                .path(file.getAbsolutePath())
                .length(file.length())
                .lastModified(file.lastModified());
        return tapFile;
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws IOException {
        if (!isFileExist(path)) {
            return;
        }
        try (
                InputStream is = Files.newInputStream(Paths.get(path))
        ) {
            consumer.accept(is);
        }
    }

    @Override
    public boolean isFileExist(String path) {
        File file = new File(path);
        return file.exists() && file.isFile();
    }

    @Override
    public boolean move(String sourcePath, String destPath) throws IOException {
        Files.move(Paths.get(sourcePath), Paths.get(destPath));
        return true;
    }

    @Override
    public boolean delete(String path) throws IOException {
        return Files.deleteIfExists(Paths.get(path));
    }

    @Override
    public TapFile saveFile(String path, InputStream is, boolean canReplace) throws IOException {
        if (isDirectoryExist(path)) {
            return null;
        }
        if (isFileExist(path) && !canReplace) {
            return getFile(path);
        }
        OutputStream os = Files.newOutputStream(Paths.get(path));
        int bytesRead;
        byte[] buffer = new byte[8192];
        while ((bytesRead = is.read(buffer, 0, 8192)) != -1) {
            os.write(buffer, 0, bytesRead);
        }
        os.flush();
        os.close();
        return getFile(path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) throws Exception {
        return new FileOutputStream(path, append);
    }

    @Override
    public void getFilesInDirectory(String directoryPath,
                                    Collection<String> includeRegs,
                                    Collection<String> excludeRegs,
                                    boolean recursive,
                                    int batchSize,
                                    Consumer<List<TapFile>> consumer) {
        if (!isDirectoryExist(directoryPath)) {
            return;
        }
        AtomicReference<List<TapFile>> listAtomicReference = new AtomicReference<>(new ArrayList<>());
        getFiles(directoryPath, includeRegs, excludeRegs, recursive, batchSize, consumer, listAtomicReference);
        if (listAtomicReference.get().size() > 0) {
            consumer.accept(listAtomicReference.get());
        }
    }

    private void getFiles(String directoryPath,
                          Collection<String> includeRegs,
                          Collection<String> excludeRegs,
                          boolean recursive,
                          int batchSize,
                          Consumer<List<TapFile>> consumer,
                          AtomicReference<List<TapFile>> listAtomicReference) {
        File dir = new File(directoryPath);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isFile()) {
                if (FileMatchKit.matchRegs(file.getName(), includeRegs, excludeRegs)) {
                    listAtomicReference.get().add(toTapFile(file));
                    if (listAtomicReference.get().size() >= batchSize) {
                        consumer.accept(listAtomicReference.get());
                        listAtomicReference.set(new ArrayList<>());
                    }
                }
            } else if (recursive) {
                getFiles(file.getAbsolutePath(), includeRegs, excludeRegs, true, batchSize, consumer, listAtomicReference);
            }
        }
    }

    @Override
    public boolean isDirectoryExist(String path) {
        File file = new File(path);
        return file.exists() && file.isDirectory();
    }

    @Override
    public String getConnectInfo() {
        return "local:";
    }

}
