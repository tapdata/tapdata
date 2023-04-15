package io.tapdata.connector.tencent.db.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author GavinXiao
 * @description Set2 create by Gavin
 * @create 2023/4/15 17:49
 **/
public class Set2 {
    public static String[] getSets(Map<String, Object> mysqlConfig) throws Exception {
        return new String[]{"set_1681181762_3"};
    }
    public static void main(String[] args) throws Exception {
        Map<String, Object> mysqlConfig = new HashMap<>();
        mysqlConfig.put("host", "gz-tdsqlshard-mc4o2pmv.sql.tencentcdb.com");
        mysqlConfig.put("port", 23115);
        mysqlConfig.put("user", "tapdata");
        mysqlConfig.put("password", "Gotapd8!");

        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger u = new AtomicInteger(0);
        AtomicInteger d = new AtomicInteger(0);

        String[] sets = getSets(mysqlConfig);

        BinaryLogClient client = new BinaryLogClient(
                "gz-tdsqlshard-mc4o2pmv.sql.tencentcdb.com",
                23115,
                "test",
                "tapdata",
                "Gotapd8!",
                "/*proxy*/ set binlog_dump_sticky_backend=" + sets[0]);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(event -> {
            EventData data = event.getData();
            if (data instanceof UpdateRowsEventData) {
                UpdateRowsEventData eventData = (UpdateRowsEventData) data;
                List<Map.Entry<Serializable[], Serializable[]>> rows = eventData.getRows();
                Map<Serializable[], Serializable[]> map = new HashMap<>();
                for (Map.Entry<Serializable[], Serializable[]> row : rows) {
                    map.put(row.getKey(), row.getValue());
                }
                System.out.println("update:" + u.addAndGet(1) + data) ;
            }
            else if (data instanceof WriteRowsEventData) {
                WriteRowsEventData eventData = (WriteRowsEventData) data;
                List<Serializable[]> rows = eventData.getRows();
                Map<Serializable[], Serializable[]> map = new HashMap<>();
                for (Serializable[] row : rows) {
                    map.put(row, row);
                }
                System.out.println("insert:" + i.addAndGet(1) +data);
            }
            else if (data instanceof DeleteRowsEventData) {
                DeleteRowsEventData eventData = (DeleteRowsEventData) data;
                List<Serializable[]> rows = eventData.getRows();
                Map<Serializable[], Serializable[]> map = new HashMap<>();
                for (Serializable[] row : rows) {
                    map.put(row, row);
                }
                System.out.println("delete:" + d.addAndGet(1) + data);
            }
        });

        client.connect(3000);
    }
}
