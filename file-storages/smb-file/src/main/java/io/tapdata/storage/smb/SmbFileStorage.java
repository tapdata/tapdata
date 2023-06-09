package io.tapdata.storage.smb;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;
import io.tapdata.storage.kit.FileMatchKit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class SmbFileStorage implements TapFileStorage {

    private SmbConfig smbConfig;
    private SMBClient smbClient;
    private Connection connection;
    private Session session;
    private DiskShare share;

    @Override
    public void init(Map<String, Object> params) throws IOException {
        smbConfig = new SmbConfig().load(params);
        smbClient = new SMBClient();
        connection = smbClient.connect(smbConfig.getSmbHost());
        AuthenticationContext ac;
        if (EmptyKit.isNotBlank(smbConfig.getSmbUsername())) {
            ac = new AuthenticationContext(smbConfig.getSmbUsername(), smbConfig.getSmbPassword().toCharArray(), smbConfig.getSmbDomain());
        } else {
            ac = AuthenticationContext.anonymous();
        }
        session = connection.authenticate(ac);
        share = (DiskShare) session.connectShare(smbConfig.getSmbShareDir());
    }

    @Override
    public void destroy() throws IOException {
        if (EmptyKit.isNotNull(share)) {
            share.close();
        }
        if (EmptyKit.isNotNull(session)) {
            session.close();
        }
        if (EmptyKit.isNotNull(connection)) {
            connection.close();
        }
        if (EmptyKit.isNotNull(smbClient)) {
            smbClient.close();
        }
    }

    @Override
    public TapFile getFile(String path) {
        if (!isFileExist(path) && !isDirectoryExist(path)) {
            return null;
        } else {
            FileAllInformation fileInfo = share.getFileInformation(path);
            TapFile tapFile = new TapFile();
            tapFile.type(isFileExist(path) ? TapFile.TYPE_FILE : TapFile.TYPE_DIRECTORY)
                    .name(path.substring(path.lastIndexOf("\\") + 1))
                    .path(path)
                    .length(fileInfo.getStandardInformation().getAllocationSize())
                    .lastModified(fileInfo.getBasicInformation().getLastWriteTime().toEpoch(TimeUnit.MILLISECONDS));
            return tapFile;
        }
    }

    private TapFile toTapFile(FileIdBothDirectoryInformation smbFile, String path) {
        TapFile tapFile = new TapFile();
        tapFile.type(TapFile.TYPE_FILE)
                .name(smbFile.getFileName())
                .path(path)
                .length(smbFile.getAllocationSize())
                .lastModified(smbFile.getLastWriteTime().toEpoch(TimeUnit.MILLISECONDS));
        return tapFile;
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws IOException {
        if (!isFileExist(path)) {
            return;
        }
        try (
                InputStream is = share.openFile(path, EnumSet.of(AccessMask.GENERIC_READ), null,
                        SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null).getInputStream()
        ) {
            consumer.accept(is);
        }
    }

    @Override
    public boolean isFileExist(String path) {
        return share.fileExists(path);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(String path) {
        try {
            if (isFileExist(path)) {
                share.rm(path);
                return true;
            } else if (isDirectoryExist(path)) {
                share.rmdir(path, true);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public TapFile saveFile(String path, InputStream is, boolean canReplace) throws IOException {
        if (!isFileExist(path) || canReplace) {
            File file = share.openFile(path, new HashSet<>(Collections.singletonList(AccessMask.GENERIC_ALL)), null,
                    SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OVERWRITE_IF, null);
            OutputStream os = file.getOutputStream();
            int bytesRead;
            byte[] buffer = new byte[8192];
            while ((bytesRead = is.read(buffer, 0, 8192)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.flush();
            os.close();
        }
        return getFile(path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) {
        return share.openFile(path, new HashSet<>(Collections.singletonList(AccessMask.GENERIC_ALL)), null,
                SMB2ShareAccess.ALL, append ? SMB2CreateDisposition.FILE_OPEN_IF : SMB2CreateDisposition.FILE_OVERWRITE_IF, null).getOutputStream();
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
        for (FileIdBothDirectoryInformation smbFile : share.list(directoryPath)) {
            String fileName = smbFile.getFileName();
            if (fileName.equals(".") || fileName.equals("..")) {
                continue;
            }
            if (!isDir(smbFile.getFileAttributes())) {
                if (FileMatchKit.matchRegs(fileName, includeRegs, excludeRegs)) {
                    listAtomicReference.get().add(toTapFile(smbFile, getAbsolutePath(directoryPath, fileName)));
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
    public boolean isDirectoryExist(String path) {
        return share.folderExists(path);
    }

    @Override
    public String getConnectInfo() {
        return "smb://" + smbConfig.getSmbHost() + "/" + smbConfig.getSmbShareDir() + "/";
    }

    private String getAbsolutePath(String parentPath, String fileName) {
        if (parentPath.endsWith("\\")) {
            return parentPath + fileName;
        } else {
            return parentPath + "\\" + fileName;
        }
    }

    private boolean isDir(long attrs) {
        return (attrs & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) == FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue();
    }

}
