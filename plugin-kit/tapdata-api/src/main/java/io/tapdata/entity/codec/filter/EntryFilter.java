package io.tapdata.entity.codec.filter;

public interface EntryFilter {
    Object filter(String key, Object value, boolean recursive);
}
