package io.tapdata.file;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
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
    void init(Map<String, Object> params) throws Exception;

    ;

    /**
     * Release current resources.
     */
    void destroy() throws Exception;

    ;

    /**
     * Get file by path. The path should be relative path without protocol. like
     * /root/app/tapdata/1.png
     * <p>
     * should not be
     * c:\root\app\tapdata\1.png
     * file://root/app/tapdata/1.png
     * <p>
     * if file/directory is not exist on the path, return null, shall avoid to throw exception.
     *
     * @param path
     * @return
     */
    TapFile getFile(String path) throws Exception;

    /**
     * Read a path into InputStream to transfer the file bytes into network or other storages.
     * <p>
     * InputStream need to be closed when finish using.
     * <p>
     * if file is not exists on path, then return null.
     *
     * @param path
     * @return
     */
    void readFile(String path, Consumer<InputStream> consumer) throws Exception;

    /**
     * Check the file is exists or not on the path.
     * Directory is not file, if path is directory, then should return false.
     *
     * @param path
     * @return
     */
    boolean isFileExist(String path) throws Exception;

    /**
     * Move file or directory from sourcePath to destPath.
     * Return true means move successfully， otherwise is false.
     * <p>
     * If either sourcePath or destPath is not exist, shall return false.
     *
     * @param sourcePath
     * @param destPath
     * @return
     */
    boolean move(String sourcePath, String destPath) throws Exception;

    /**
     * Delete file on the path.
     * If actually deleted, return true, otherwise return false.
     *
     * @param path
     * @return
     */
    boolean delete(String path) throws Exception;

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
    TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception;

    OutputStream openFileOutputStream(String path, boolean append) throws Exception;

    default boolean supportAppendData() {
        return true;
    }

    /**
     * Get files/directories on path "directoryPath" with matching regression "matchingReg".
     * If directoryPath is not exists or is a file, then throw CoreException with specified code.
     *
     * @param directoryPath
     * @param includeRegs   can be null
     * @param excludeRegs   can be null
     * @param recursive
     * @param batchSize     the max batch size when consumer accept a batch TapFile.
     * @param consumer      accept a batch of TapFile.
     */
    void getFilesInDirectory(String directoryPath, Collection<String> includeRegs, Collection<String> excludeRegs, boolean recursive, int batchSize, Consumer<List<TapFile>> consumer) throws Exception;

    /**
     * Check the directory is exists or not on the path.
     * File is not directory, if path is file, then should return false.
     *
     * @param path
     * @return
     */
    boolean isDirectoryExist(String path) throws Exception;

    String getConnectInfo();
}