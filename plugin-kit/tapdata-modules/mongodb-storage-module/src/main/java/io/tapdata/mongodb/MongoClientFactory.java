package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Bean
public class MongoClientFactory {
    
    private final String TAG = MongoClientFactory.class.getSimpleName();
    
    private final Map<String, MongoClientHolder> clientMap = new ConcurrentHashMap<>();

    @Bean
    private MongoClientFactory instance;
    
    public MongoClientFactory() {
    }
    
    /**
     * 获取client
     * @param url mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet
     *            mongodb://user1:pwd1@host1/?authSource=db1&ssl=true
     * @param name
     * @return
     */
    public MongoClientHolder getClient(String url, String name) {
        MongoClientHolder mongoClient = clientMap.get(name);
        if (mongoClient == null) {
            synchronized (this) {
                mongoClient = clientMap.get(name);
                if(mongoClient == null) {
                    ConnectionString connectionString = new ConnectionString(url);
                    mongoClient = new MongoClientHolder().mongoClient(MongoClients.create(connectionString)).connectionString(connectionString);

                    clientMap.putIfAbsent(name, mongoClient);

                    TapLogger.debug(TAG, "Connected mongodb, name " + name + "");
                }
            }
        }
        return clientMap.get(name);
    }
}
