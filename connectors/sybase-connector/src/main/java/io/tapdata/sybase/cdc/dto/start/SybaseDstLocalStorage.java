package io.tapdata.sybase.cdc.dto.start;

/**
 * @author GavinXiao
 * @description SybaseDstLocalStorage create by Gavin
 * @create 2023/7/13 17:00
 **/
public class SybaseDstLocalStorage {
    String type;
    String storage_location;
    String file_format;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStorage_location() {
        return storage_location;
    }

    public void setStorage_location(String storage_location) {
        this.storage_location = storage_location;
    }

    public String getFile_format() {
        return file_format;
    }

    public void setFile_format(String file_format) {
        this.file_format = file_format;
    }
}
