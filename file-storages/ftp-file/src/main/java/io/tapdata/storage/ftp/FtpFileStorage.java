package io.tapdata.storage.ftp;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;
import io.tapdata.storage.kit.FileMatchKit;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FtpFileStorage implements TapFileStorage {

    private FtpConfig ftpConfig;
    private FTPClient ftpClient;

    @Override
    public void init(Map<String, Object> params) throws IOException {
        ftpConfig = new FtpConfig().load(params);
        ftpClient = new FTPClient();
        ftpClient.setConnectTimeout(ftpConfig.getFtpConnectTimeout());
        ftpClient.setDataTimeout(ftpConfig.getFtpDataTimeout());
        ftpClient.connect(ftpConfig.getFtpHost(), ftpConfig.getFtpPort());
        if (ftpConfig.getFtpSsl() && EmptyKit.isNotBlank(ftpConfig.getFtpAccount())) {
            ftpClient.login(ftpConfig.getFtpUsername(), ftpConfig.getFtpPassword(), ftpConfig.getFtpAccount());
        } else {
            ftpClient.login(ftpConfig.getFtpUsername(), ftpConfig.getFtpPassword());
        }
        if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
            ftpClient.setControlEncoding(ftpConfig.getEncoding());
            if (ftpConfig.getFtpPassiveMode()) {
                ftpClient.enterLocalPassiveMode();
            }
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        } else {
            ftpClient.disconnect();
            throw new IOException(String.format("connect to ftp server failed! code:%s", ftpClient.getReplyCode()));
        }

    }

    @Override
    public void destroy() throws IOException {
        if (EmptyKit.isNotNull(ftpClient)) {
            if (ftpClient.isConnected()) {
                ftpClient.logout();
                ftpClient.disconnect();
            }
        }
    }

    @Override
    public synchronized TapFile getFile(String path) throws IOException {
        String fileName = path.substring(path.lastIndexOf("/") + 1);
        if (isDirectoryExist(path)) {
            ftpClient.changeToParentDirectory();
            FTPFile ftpFile = Arrays.stream(ftpClient.listDirectories())
                    .filter(f -> fileName.equals(f.getName())).findFirst().orElseGet(FTPFile::new);
            TapFile tapFile = new TapFile();
            tapFile.type(TapFile.TYPE_DIRECTORY)
                    .name(fileName)
                    .path(path)
                    .length(ftpFile.getSize())
                    .lastModified(ftpFile.getTimestamp().getTimeInMillis());
            return tapFile;
        } else {
            FTPFile[] ftpFiles = ftpClient.listFiles(encodeISO(path));
            if (ftpFiles.length != 1) {
                return null;
            } else {
                return toTapFile(ftpFiles[0], path);
            }
        }
    }

    private TapFile toTapFile(FTPFile file, String path) {
        TapFile tapFile = new TapFile();
        tapFile.type(TapFile.TYPE_FILE)
                .name(file.getName())
                .path(path)
                .length(file.getSize())
                .lastModified(file.getTimestamp().getTimeInMillis());
        return tapFile;
    }

    @Override
    public synchronized void readFile(String path, Consumer<InputStream> consumer) throws IOException {
        if (!isFileExist(path)) {
            return;
        }
        try (
                InputStream is = ftpClient.retrieveFileStream(encodeISO(path))
        ) {
            consumer.accept(is);
        } finally {
            ftpClient.completePendingCommand();
        }
    }

    @Override
    public boolean isFileExist(String path) throws IOException {
        return !isDirectoryExist(path) && ftpClient.listFiles(encodeISO(path)).length == 1;
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(String path) throws IOException {
        return ftpClient.deleteFile(encodeISO(path));
    }

    @Override
    public synchronized TapFile saveFile(String path, InputStream is, boolean canReplace) throws IOException {
        if (!isFileExist(path) || canReplace) {
            ftpClient.changeWorkingDirectory(encodeISO(path.substring(0, path.lastIndexOf("/"))));
            ftpClient.storeFile(encodeISO(path.substring(path.lastIndexOf("/") + 1)), is);
        }
        return getFile(path);
    }

    public OutputStream openFileOutputStream(String path, boolean append) throws IOException {
        OutputStream os;
        if (append) {
            os = ftpClient.appendFileStream(encodeISO(path));
        } else {
            os = ftpClient.storeFileStream(encodeISO(path));
        }

        return new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                os.write(b);
            }

            @Override
            public void close() throws IOException {
                os.close();
                ftpClient.completePendingCommand();
            }

        };
    }

    @Override
    public void getFilesInDirectory(String directoryPath,
                                    Collection<String> includeRegs,
                                    Collection<String> excludeRegs,
                                    boolean recursive, int batchSize,
                                    Consumer<List<TapFile>> consumer) throws IOException {
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
                          AtomicReference<List<TapFile>> listAtomicReference) throws IOException {
        for (FTPFile ftpFile : Objects.requireNonNull(ftpClient.listFiles(encodeISO(directoryPath)))) {
            if (ftpFile.isFile()) {
                if (FileMatchKit.matchRegs(ftpFile.getName(), includeRegs, excludeRegs)) {
                    listAtomicReference.get().add(toTapFile(ftpFile, getAbsolutePath(directoryPath, ftpFile.getName())));
                    if (listAtomicReference.get().size() >= batchSize) {
                        consumer.accept(listAtomicReference.get());
                        listAtomicReference.set(new ArrayList<>());
                    }
                }
            } else if (recursive) {
                getFiles(getAbsolutePath(directoryPath, ftpFile.getName()), includeRegs, excludeRegs, true, batchSize, consumer, listAtomicReference);
            }
        }
    }

    @Override
    public synchronized boolean isDirectoryExist(String path) throws IOException {
        return ftpClient.changeWorkingDirectory(encodeISO(path));
    }

    @Override
    public String getConnectInfo() {
        return "ftp://" + ftpConfig.getFtpHost() + (ftpConfig.getFtpPort() == 21 ? "" : (":" + ftpConfig.getFtpPort())) + "/";
    }

    private String encodeISO(String path) {
        try {
            return new String(path.getBytes(ftpConfig.getEncoding()), StandardCharsets.ISO_8859_1);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getAbsolutePath(String parentPath, String fileName) {
        if (parentPath.endsWith("/")) {
            return parentPath + fileName;
        } else {
            return parentPath + "/" + fileName;
        }
    }

}
