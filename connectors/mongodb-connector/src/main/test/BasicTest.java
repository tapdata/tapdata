import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.internal.MongoBatchCursorAdapter;
import io.tapdata.mongodb.entity.MongoDBConfig;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@DisplayName("Bench Test")
class BasicTest {
    private MongoClient mongoClient;


    private void initConnection() throws Exception {
        if (mongoClient == null) {
            String configPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\mongodb-connector\\src\\main\\resources\\config.json";
            MongoDBConfig mongoDBConfig = MongoDBConfig.load(configPath);
            mongoClient = MongoClients.create("mongodb://" + mongoDBConfig.getHost() + ":" + mongoDBConfig.getPort());
        }
    }


    @Test
    void testConnect() {
        try {
            initConnection();
            //连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
//            MongoCollection<Document> collection = mongoDatabase.getCollection("test");
//            System.out.println("document count:" + collection.countDocuments());

//            MongoBatchCursorAdapter<Document> mongoCursor = (MongoBatchCursorAdapter<Document>) collection.find().batchSize(2000).iterator();
//            int cnt = 1;
//            int eventBatchSize = 1000;
//            List<Document> documentList = new ArrayList<>();
//            while (mongoCursor.hasNext()) {
//                Document document = mongoCursor.next();
//                documentList.add(document);
//                if (mongoCursor.available() - 1 % eventBatchSize == 0) {
//                    System.out.println(cnt++);
//                    // 发送事件
//                }
////                System.out.println(mongoCursor.next());
////                System.out.println(mongoCursor.available());
//            }

            BasicDBObject dbStats = new BasicDBObject("usersInfo",1);
            Document document = mongoDatabase.runCommand(dbStats);
            List<Document> users = (ArrayList<Document>) document.get("users");
            for (Document user : users) {
                if(!Objects.equals(user.get("user").toString(), "usertest")) continue;
                List<Document> roleInfos = (ArrayList<Document>) user.get("roles");
                for (Document roleInfo : roleInfos) {
                    String role = roleInfo.get("role").toString();
                    String db = roleInfo.get("db").toString();
                    if(Objects.equals(db, "test") && Objects.equals(role, "readWrite")){
                        System.out.println(roleInfo);
                        break;
                    }
                }
            }
            System.out.println();
            System.out.println("Connect to database successfully");
            mongoClient.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

}