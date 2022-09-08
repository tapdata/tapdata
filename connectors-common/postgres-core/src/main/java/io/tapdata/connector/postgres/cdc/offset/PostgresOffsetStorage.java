package io.tapdata.connector.postgres.cdc.offset;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresOffsetStorage {

    public static Map<String, PostgresOffset> postgresOffsetMap = new ConcurrentHashMap<>(); //one slot one key

}
