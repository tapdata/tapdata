package io.tapdata.connector.gauss.offset;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GaussOffsetStorage {
    public static Map<String, GaussOffset> guassOffsetMap = new ConcurrentHashMap<>(); //one slot one key
}
