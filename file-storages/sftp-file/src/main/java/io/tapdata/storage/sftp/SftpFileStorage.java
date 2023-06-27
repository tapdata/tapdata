package io.tapdata.storage.sftp;

import com.jcraft.jsch.*;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;
import io.tapdata.storage.kit.FileMatchKit;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class SftpFileStorage implements TapFileStorage {

    private SftpConfig sftpConfig;
    private Session session;
    private ChannelSftp channel;

    @Override
    public void init(Map<String, Object> params) throws JSchException, SftpException {
        sftpConfig = new SftpConfig().load(params);
        JSch jsch = new JSch();
        session = jsch.getSession(sftpConfig.getSftpUsername(), sftpConfig.getSftpHost(), sftpConfig.getSftpPort());
        if (EmptyKit.isNotBlank(sftpConfig.getSftpPassword())) {
            session.setPassword(sftpConfig.getSftpPassword());
        }
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.setTimeout(10000);
        session.connect();
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect();
        channel.setFilenameEncoding(sftpConfig.getEncoding());
    }

    @Override
    public void destroy() {
        if (EmptyKit.isNotNull(channel)) {
            channel.disconnect();
        }
        if (EmptyKit.isNotNull(session)) {
            session.disconnect();
        }
    }

    @Override
    public TapFile getFile(String path) throws SftpException {
        if (!isDirectoryExist(path) && !isFileExist(path)) {
            return null;
        } else {
            SftpATTRS attrs = channel.stat(path);
            TapFile tapFile = new TapFile();
            tapFile.type(isDirectoryExist(path) ? TapFile.TYPE_DIRECTORY : TapFile.TYPE_FILE)
                    .name(path.substring(path.lastIndexOf("/") + 1))
                    .path(path)
                    .length(attrs.getSize())
                    .lastModified(attrs.getMTime() * 1000L);
            return tapFile;
        }
    }

    private TapFile toTapFile(ChannelSftp.LsEntry lsEntry, String path) {
        TapFile tapFile = new TapFile();
        tapFile.type(TapFile.TYPE_FILE)
                .name(lsEntry.getFilename())
                .path(path)
                .length(lsEntry.getAttrs().getSize())
                .lastModified(lsEntry.getAttrs().getMTime() * 1000L);
        return tapFile;
    }

    @Override
    public synchronized void readFile(String path, Consumer<InputStream> consumer) throws Exception {
        if (!isFileExist(path)) {
            return;
        }
        try (
                InputStream is = channel.get(path)
        ) {
            consumer.accept(is);
        }
    }

    @Override
    public boolean isFileExist(String path) {
        try {
            return !channel.stat(path).isDir();
        } catch (SftpException e) {
            return false;
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(String path) {
        try {
            if (isFileExist(path)) {
                channel.rm(path);
                return true;
            } else if (isDirectoryExist(path)) {
                deleteDir(path);
                return true;
            } else {
                return false;
            }
        } catch (SftpException e) {
            return false;
        }
    }

    private void deleteDir(String path) throws SftpException {
        for (Object o : channel.ls(path)) {
            ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) o;
            String fileName = lsEntry.getFilename();
            if (".".equals(fileName) || "..".equals(fileName)) {
                continue;
            }
            if (lsEntry.getAttrs().isDir()) {
                deleteDir(getAbsolutePath(path, fileName));
            } else {
                channel.rm(getAbsolutePath(path, fileName));
            }
        }
        channel.rmdir(path);
    }

    @Override
    public TapFile saveFile(String path, InputStream is, boolean canReplace) throws SftpException {
        if (!isFileExist(path) || canReplace) {
            channel.put(is, path, ChannelSftp.OVERWRITE);
        }
        return getFile(path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) throws Exception {
        return channel.put(path, append ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE);
    }

    @Override
    public void getFilesInDirectory(String directoryPath,
                                    Collection<String> includeRegs,
                                    Collection<String> excludeRegs,
                                    boolean recursive,
                                    int batchSize,
                                    Consumer<List<TapFile>> consumer) throws SftpException {
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
                          AtomicReference<List<TapFile>> listAtomicReference) throws SftpException {
        for (Object o : channel.ls(directoryPath)) {
            ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) o;
            String fileName = lsEntry.getFilename();
            if (fileName.equals(".") || fileName.equals("..")) {
                continue;
            }
            if (!lsEntry.getAttrs().isDir()) {
                if (FileMatchKit.matchRegs(fileName, includeRegs, excludeRegs)) {
                    listAtomicReference.get().add(toTapFile(lsEntry, getAbsolutePath(directoryPath, fileName)));
                    if (listAtomicReference.get().size() >= batchSize) {
                        consumer.accept(listAtomicReference.get());
                        listAtomicReference.set(new ArrayList<>());
                    }
                }
            } else if (recursive) {
                getFiles(getAbsolutePath(directoryPath, fileName), includeRegs, excludeRegs, true, batchSize, consumer, listAtomicReference);
            }
        }
    }

    @Override
    public synchronized boolean isDirectoryExist(String path) {
        try {
            channel.cd(path);
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    @Override
    public String getConnectInfo() {
        return "sftp://" + sftpConfig.getSftpHost() + (sftpConfig.getSftpPort() == 22 ? "" : (":" + sftpConfig.getSftpPort())) + "/";
    }

    private String getAbsolutePath(String parentPath, String fileName) {
        if (parentPath.endsWith("/")) {
            return parentPath + fileName;
        } else {
            return parentPath + "/" + fileName;
        }
    }
}
