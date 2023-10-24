package io.tapdata.pdk.apis.entity;

public class SortOn {
    public static final int ASCENDING = 1;
    public static final int DESCENDING = 2;
    private String key;
    private int sort;

    public SortOn() {}
    
    public SortOn(String key, int sort) {
        this.key = key;
        this.sort = sort;
    }

    public static SortOn ascending(String key) {
        return new SortOn(key, ASCENDING);
    }

    public static SortOn descending(String key) {
        return new SortOn(key, DESCENDING);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public String toString() {
        return toString("");
    }

    public String toString(String quote) {
        return quote + key + quote + " " + (sort == ASCENDING ? "ASC" : "DESC");
    }
}
