package io.tapdata.connector.tdengine.subscribe;

import com.taosdata.jdbc.tmq.ReferenceDeserializer;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TDengineResultDeserializer extends ReferenceDeserializer<Map<String, Object>> {
    @Override
    public Map<String, Object> deserialize(ResultSet data) {
        Map<String, Object> map = new HashMap<>();

        try {
            ResultSetMetaData metaData = data.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                map.put(metaData.getColumnLabel(i), data.getObject(i));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return map;
    }
}
