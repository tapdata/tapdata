package io.tapdata.connector.open.gauss.postgres.cdc.offset;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OpenGaussOffsetStorage {

    public static Map<String, OpenGaussOffset> postgresOffsetMap = new ConcurrentHashMap<>(); //one slot one key

}
