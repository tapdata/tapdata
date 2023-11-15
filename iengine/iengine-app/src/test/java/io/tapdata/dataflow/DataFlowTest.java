package io.tapdata.dataflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataFlowTest {

	private Logger logger = LogManager.getLogger(DataFlowTest.class);

	public String tapdataMongoDBURI = "mongodb://localhost/tapdata";

//    @Test
//    public void customDataFlowTest() throws Exception {
////        URL jobsURL = DataFlowTest.class.getClassLoader().getResource("json/DataFlow.json");
////        String dataFlowJson = new String(Files.readAllBytes(Paths.get(jobsURL.toURI())));
////
////        MongoClientURI mongoClientURI = new MongoClientURI(tapdataMongoDBURI);
////        MongoClient mongoClient = new MongoClient(mongoClientURI);
////
////
////        MongoCollection<Document> dataFlows = mongoClient.getDatabase(mongoClientURI.getDatabase()).getCollection("DataFlows");
////
////        Document dataFlowsDoc = Document.parse(dataFlowJson);
////
////        Document dataFlowNameFileter = new Document("name", dataFlowsDoc.getString("name"));
////        dataFlows.updateOne(dataFlowNameFileter, new Document("$set", dataFlowsDoc), new UpdateOptions().upsert(true));
////        dataFlows.updateOne(dataFlowNameFileter, new Document("$set", new Document("status", "scheduled")));
//
////        while (true) {
////            long countDocuments = dataFlows.countDocuments(dataFlowNameFileter.append("status", "running"));
////            if (countDocuments >= 1) {
////                System.out.println("Data flow " + dataFlowsDoc.getString("name") + " start success, status.");
////
////                break;
////            }
////
////            System.out.println("Waiting data flow " + dataFlowsDoc.getString("name") + " start up.");
////            Thread.sleep(1000);
////        }
//    }
}
