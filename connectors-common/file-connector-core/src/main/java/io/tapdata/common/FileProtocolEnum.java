package io.tapdata.common;

public enum FileProtocolEnum {

    LOCAL("local", "io.tapdata.storage.local.LocalFileStorage"),
    FTP("ftp", "io.tapdata.storage.ftp.FtpFileStorage"),
    SFTP("sftp", "io.tapdata.storage.sftp.SftpFileStorage"),
    SMB("smb", "io.tapdata.storage.smb.SmbFileStorage"),
    S3FS("s3fs", "io.tapdata.storage.s3fs.S3fsFileStorage"),
    NFS("nfs", "io.tapdata.storage.nfs.NfsFileStorage"),
    OSS("oss","io.tapdata.storage.oss.OssFileStorage"),
    UNSUPPORTED("unsupported", null);

    private final String name;
    private final String storage;

    FileProtocolEnum(String name, String storage) {
        this.name = name;
        this.storage = storage;
    }

    public String getName() {
        return name;
    }

    public String getStorage() {
        return storage;
    }

    public static FileProtocolEnum fromValue(String name) {
        for (FileProtocolEnum protocol : FileProtocolEnum.values()) {
            if (protocol.getName().equals(name)) {
                return protocol;
            }
        }
        return UNSUPPORTED;
    }
}
