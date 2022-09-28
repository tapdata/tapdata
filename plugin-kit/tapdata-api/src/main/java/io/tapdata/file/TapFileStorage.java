package io.tapdata.file;

import io.tapdata.entity.error.CoreException;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 *
 */
public interface TapFileStorage {
    /**
     * Different protocol implementation need different parameters.
     *
     * @param params should be provided from spec.json' json schema.
     */
    void init(Map<String, Object> params);

    /**
     * Release current resources.
     */
    void destroy();

    /**
     * Get file by path. The path should be relative path without protocol. like
     * /root/app/tapdata/1.png
     *
     * should not be
     * c:\root\app\tapdata\1.png
     * file://root/app/tapdata/1.png
     *
     * if file/directory is not exist on the path, return null, shall avoid to throw exception.
     *
     * @param path
     * @return
     */
    TapFile getFile(String path);

    /**
     * Read a path into InputStream to transfer the file bytes into network or other storages.
     *
     * InputStream need to be closed when finish using.
     *
     * if file is not exists on path, then return null.
     *
     * @param path
     * @return
     */
    InputStream readFile(String path);

    /**
     * Check the file is exists or not on the path.
     * Directory is not file, if path is directory, then should return false.
     *
     * @param path
     * @return
     */
    boolean isFileExist(String path);

    /**
     * Move file or directory from sourcePath to destPath.
     * Return true means move successfully， otherwise is false.
     *
     * If either sourcePath or destPath is not exist, shall return false.
     *
     * @param sourcePath
     * @param destPath
     * @return
     */
    boolean move(String sourcePath, String destPath);

    /**
     * Delete file on the path.
     * If actually deleted, return true, otherwise return false.
     *
     * @param path
     * @return
     */
    boolean delete(String path);

    /**
     * Save file into path.
     * canReplace is true means when file already exists, delete file and save file, otherwise ignore new file.
     * If path is a directory, throw CoreException with specified error code which stand for path is directory error.
     *
     * @param path
     * @param is
     * @param canReplace
     * @return
     */
    TapFile saveFile(String path, InputStream is, boolean canReplace);

    /**
     * Get files/directories on path "directoryPath" with matching regression "matchingReg".
     * If directoryPath is not exists or is a file, then throw CoreException with specified code.
     *
     * @param directoryPath
     * @param includeReg can be null
     * @param excludeReg can be null
     * @param recursive
     * @param batchSize the max batch size when consumer accept a batch TapFile.
     * @param consumer accept a batch of TapFile.
     */
    void getFilesInDirectory(String directoryPath, String includeReg, String excludeReg, boolean recursive, int batchSize, Consumer<List<TapFile>> consumer);

    /**
     * Check the directory is exists or not on the path.
     * File is not directory, if path is file, then should return false.
     *
     * @param path
     * @return
     */
    boolean isDirectoryExist(String path);


}