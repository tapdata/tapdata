package io.tapdata.sybase.cdc.dto.read;

/**
 * @author GavinXiao
 * @description TableTypeEntity create by Gavin
 * @create 2023/7/31 15:19
 **/
public class TableTypeEntity {
    String type;
    String name;
    int length;

    public TableTypeEntity(String type, String name, int length) {
        this.type = type;
        this.name = name;
        this.length = length;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
