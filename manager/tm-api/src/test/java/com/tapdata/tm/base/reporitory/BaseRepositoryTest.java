package com.tapdata.tm.base.reporitory;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for BaseRepository
 */
public class BaseRepositoryTest {

    @Test
    public void testAddOrFilter_WithEmptyQuery() throws Exception {
        Query query = new Query();
        Criteria criteria1 = Criteria.where("field1").is("value1");
        Criteria criteria2 = Criteria.where("field2").is("value2");
        
        BaseRepository.addOrFilter(query, criteria1, criteria2);
        
        Document queryDoc = query.getQueryObject();
        assertNotNull(queryDoc);
        assertTrue(queryDoc.containsKey("$or"));
    }

    @Test
    public void testAddOrFilter_WithExistingOrCondition() throws Exception {
        Query query = new Query();

        Criteria searchCriteria1 = Criteria.where("name").regex("test", "i");
        Criteria searchCriteria2 = Criteria.where("dataFlowName").regex("test", "i");
        query.addCriteria(new Criteria().orOperator(searchCriteria1, searchCriteria2));

        Criteria permissionCriteria1 = Criteria.where("user_id").is("67c69c816b8259160f13a5cb");
        Criteria permissionCriteria2 = Criteria.where("permissions.type").is("Role")
                .and("permissions.typeId").in("67c69c456b8259160f13a50f")
                .and("permissions.actions").in("View");

        assertDoesNotThrow(() -> {
            BaseRepository.addOrFilter(query, permissionCriteria1, permissionCriteria2);
        });
        
        Document queryDoc = query.getQueryObject();
        assertNotNull(queryDoc);
        

        assertTrue(queryDoc.containsKey("$and"));

    }

    @Test
    public void testAddOrFilter_WithExistingNonOrCondition() throws Exception {
        Query query = new Query();
        query.addCriteria(Criteria.where("status").is("active"));
        query.addCriteria(Criteria.where("customerId").is("customer123"));

        Criteria criteria1 = Criteria.where("field1").is("value1");
        Criteria criteria2 = Criteria.where("field2").is("value2");
        
        BaseRepository.addOrFilter(query, criteria1, criteria2);
        
        Document queryDoc = query.getQueryObject();
        assertNotNull(queryDoc);
        assertTrue(queryDoc.containsKey("$or"));
        assertTrue(queryDoc.containsKey("status"));
        assertTrue(queryDoc.containsKey("customerId"));

    }

    @Test
    public void testAddOrFilter_MultipleOrConditions() throws Exception {
        Query query = new Query();

        Criteria criteria1 = Criteria.where("field1").is("value1");
        Criteria criteria2 = Criteria.where("field2").is("value2");
        BaseRepository.addOrFilter(query, criteria1, criteria2);

        Criteria criteria3 = Criteria.where("field3").is("value3");
        Criteria criteria4 = Criteria.where("field4").is("value4");
        BaseRepository.addOrFilter(query, criteria3, criteria4);
        
        Document queryDoc = query.getQueryObject();
        assertNotNull(queryDoc);
        assertTrue(queryDoc.containsKey("$and"));

    }
}
