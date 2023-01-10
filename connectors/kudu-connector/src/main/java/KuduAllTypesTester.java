import org.apache.kudu.*;
import org.apache.kudu.client.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sun.plugin2.util.PojoUtil.toJson;

/**
 * Kudu 全类型字段测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/26 上午11:30 Create
 */
public class KuduAllTypesTester implements AutoCloseable {

  private KuduClient client;
  private List<String> addressList = new ArrayList<String>() {{
    add("192.168.1.189:7051");
    add("192.168.1.189:7151");
    add("192.168.1.189:7251");
  }};

  public KuduAllTypesTester() {
    this.client = new KuduClient.KuduClientBuilder(addressList).build();
  }

  @Override
  public void close() throws Exception {
    if (null != client) {
      client.close();
    }
  }

  public void deleteTable(String tableName) throws Exception {
    client.deleteTable(tableName);
  }

  public void createTable(String tableName) throws Exception {
    // 分区配置
    CreateTableOptions options = new CreateTableOptions();
    List<String> hashKeys = new ArrayList<>(1);
    hashKeys.add("F_INT8");
    options.addHashPartitions(hashKeys, 8);

    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_INT8", Type.INT8).key(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_INT16", Type.INT16).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_INT32", Type.INT32).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_INT64", Type.INT64).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_BINARY", Type.BINARY).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_STRING", Type.STRING).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_BOOL", Type.BOOL).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_FLOAT", Type.FLOAT).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_DOUBLE", Type.DOUBLE).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_UNIXTIME_MICROS", Type.UNIXTIME_MICROS).nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_DECIMAL", Type.DECIMAL)
      .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(4).precision(5).build())
      .nullable(true).build());
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_VARCHAR", Type.VARCHAR)
      .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(500).build())
      .nullable(true).build()); // between 1 and 65535
    columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("F_DATE", Type.DATE).wireType(Common.DataType.DATE).nullable(true).build());
    Schema schema = new Schema(columnSchemas);

    KuduTable table = client.createTable(tableName, schema, options);
    System.out.println("创建表：" + table.getName());
  }

  public void showSchema(String tableName) throws Exception {
    KuduTable table = client.openTable(tableName);
    Schema schema = table.getSchema();
    for (ColumnSchema cs : schema.getColumns()) {
      System.out.println(toJson(cs));
    }
  }

  public void insert(String tableName, Map<String, Object> data) throws Exception {
    if (!client.tableExists(tableName)) throw new RuntimeException("Table not exists: " + tableName);
    KuduTable table = client.openTable(tableName);
    PartialRow row = new PartialRow(table.getSchema());
    for (Map.Entry<String, Object> en : data.entrySet()) {
      row.addObject(en.getKey(), en.getValue());
    }


    KuduSession session = null;
    try {
      session = client.newSession();

      Insert insert = table.newInsert();
      insert.setRow(row);
      OperationResponse resp = session.apply(insert);
      if (resp.hasRowError()) {
        System.out.println(resp.getRowError());
      }
      session.flush();
    } finally {
      if (null != session) {
        session.close();
      }
    }
  }

  public void insertData(String tableName) throws Exception {
    insert(tableName, new HashMap<String, Object>() {{
      put("F_INT8", (byte) 1);
      put("F_INT16", (short) 1);
      put("F_INT32", 1);
      put("F_INT64", 1L);
      put("F_BINARY", "Hello kudu.".getBytes());
      put("F_STRING", "Hello kudu.");
      put("F_BOOL", false);
      put("F_FLOAT", 1.1F);
      put("F_DOUBLE", 1.2D);
      put("F_UNIXTIME_MICROS", new Timestamp(System.currentTimeMillis()));
      put("F_DECIMAL", new BigDecimal(new BigInteger("1"), 4));
      put("F_VARCHAR", "varchar test");
      put("F_DATE", new Date(System.currentTimeMillis()));
    }});
  }

  public void showData(String tableName) throws Exception {
    Map<String, Object> msg = new HashMap<>();
    RowResult rowResult;
    RowResultIterator rowResults;
    KuduTable table = client.openTable(tableName);
    List<ColumnSchema> columnSchemas = table.getSchema().getColumns();
    KuduScanner scanner = client.newScannerBuilder(table).build();
    while (scanner.hasMoreRows()) {
      rowResults = scanner.nextRows();
      while (rowResults.hasNext()) {
        rowResult = rowResults.next();

        msg.clear();
        for (ColumnSchema cs : columnSchemas) {
          Object data = rowResult.getObject(cs.getName());
          msg.put(cs.getName(), new HashMap<String, Object>() {{
            if (null != data) {
              put("type", data.getClass());
            }
            put("value", data);
          }});
        }
        System.out.println(toJson(msg));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String tableName = "KUDU_ALL_TYPE";
    try (KuduAllTypesTester tester = new KuduAllTypesTester()) {
//      tester.deleteTable(tableName);
//      tester.createTable(tableName);
//      tester.showSchema(tableName);
//      tester.insertData(tableName);
      tester.showData(tableName);
    }
  }
}
