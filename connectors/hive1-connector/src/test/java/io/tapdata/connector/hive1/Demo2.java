//package io.tapdata.connector.hive1;
//
//public class Demo2 {
//    /*** DelimitedInputWriter使用* @throws InterruptedException* @throws StreamingException* @throws ClassNotFoundException*/
//    public static void delimitedInputWriterDemo() throws InterruptedException, StreamingException, ClassNotFoundException {
//        String dbName = "test";
//        String tblName = "t3";
//        List partitionVals = new ArrayList(1);
//        partitionVals.add("china");
//        HiveEndPoint hiveEP = new HiveEndPoint("thrift://192.168.61.146:9083", dbName, tblName, partitionVals);
//        String[] fieldNames = new String[3];
//        fieldNames[0] = "id";
//        fieldNames[1] = "name";
//        fieldNames[2] = "address";
//        StreamingConnection connection = hiveEP.newConnection(true);
//        DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames, ",", hiveEP);
//        TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);
//        txnBatch.beginNextTransaction();
//        for (int i = 0; i < 100; ++i) {
//            txnBatch.write((i + ",zhangsan,beijing").getBytes());
//        }
//        txnBatch.commit();
//        txnBatch.close();
//        connection.close();
//    }
//
//    /*** StrictJsonWriter 使用* @throws StreamingException* @throws InterruptedException*/
//    public static void strictJsonWriterDemo() throws StreamingException, InterruptedException {
//        String dbName = "test";
//        String tblName = "t3";
//        List partitionVals = new ArrayList(1);
//        partitionVals.add("china");
//        HiveEndPoint hiveEP = new HiveEndPoint("thrift://192.168.61.146:9083", dbName, tblName, partitionVals);
//        StreamingConnection connection = hiveEP.newConnection(true);
//        StrictJsonWriter writer = new StrictJsonWriter(hiveEP);
//        TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);
//        txnBatch.beginNextTransaction();
//        for (int i = 0; i < 10; ++i) {
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put("id", i);
//            jsonObject.put("name", "chenli" + i);
//            jsonObject.put("address", "beijing");
//            txnBatch.write(jsonObject.toJSONString().getBytes());
//        }
//        txnBatch.commit();
//        txnBatch.close();
//        connection.close();
//    }
//
//    public static void main(String[] args) throws InterruptedException, StreamingException, ClassNotFoundException {
//        strictJsonWriterDemo();
//    }
//}
