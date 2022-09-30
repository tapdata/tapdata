package io.tapdata.storage;

public enum FileProtocolEnum {

    LOCAL("local"),
    FTP("ftp"),
    SFTP("sftp"),
    SMB("smb"),
    NFS("nfs"),
    UNSUPPORTED("unsupported");

    private final String name;

    FileProtocolEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
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
