package com.tapdata.tm.measurementServiceV2;

import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import java.util.ArrayList;

public class MockMongoTemplate extends MongoTemplate {
    public MockMongoTemplate(MongoClient mongoClient, String databaseName) {
        super(mongoClient, databaseName);
    }

    private Aggregation aggregation;
   @Override
    public <O> AggregationResults<O> aggregate(Aggregation aggregation, String collectionName, Class<O> outputType) {
       this.aggregation = aggregation;
       return new AggregationResults(new ArrayList(), (new Document("results", "test")).append("ok", 1.0));
    }


    public Aggregation getAggregation() {
        return aggregation;
    }
}
