package io.tapdata.sybase.cdc.dto.analyse.csv.opencsv;

/**
 * @author GavinXiao
 * @description SpecialField create by Gavin
 * @create 2023/7/31 16:07
 **/
public class SpecialField {
    int index;
    String name;
    String type;
    int length;
    boolean needSpecial;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean needSpecial() {
        return this.needSpecial;
    }

    public void setNeedSpecial(boolean needSpecial) {
        this.needSpecial = needSpecial;
    }
}
