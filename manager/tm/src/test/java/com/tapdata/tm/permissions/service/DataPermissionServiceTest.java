package com.tapdata.tm.permissions.service;

import com.tapdata.tm.commons.base.DataPermissionAction;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionTypeEnums;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataPermissionServiceTest {

    @Mock
    private MongoTemplate mockMongoOperations;

    @InjectMocks
    private DataPermissionService dataPermissionServiceUnderTest;

    @BeforeEach
    void before(){
        ReflectionTestUtils.setField(dataPermissionServiceUnderTest,"mongoOperations",mockMongoOperations);
    }


    @Test
    void testDataAuth() {
        final Set<ObjectId> inputDataIds = new HashSet<>(
                Arrays.asList(MongoUtils.toObjectId("658292d19bb96967d3bbc924")));
        final DataPermissionTypeEnums inputDataPermissionTypeEnums = DataPermissionTypeEnums.Role;
        final Set<String> inputTypeIds = new HashSet<>(Arrays.asList("658292d19bb96967d3bbc924"));
        Query query = Query.query(Criteria.where("_id").in(inputDataIds));
        Update update = Update.fromDocument(new Document("$pull", new Document(DataPermissionHelper.FIELD_NAME
                , new Document("type", inputDataPermissionTypeEnums).append("typeId", new Document("$in", inputTypeIds))
        )));
        when(mockMongoOperations.updateMulti(any(Query.class),any(Update.class),any(String.class))).thenAnswer(invocationOnMock -> {
            Update resultUpdata = invocationOnMock.getArgument(1);
            if(resultUpdata.getUpdateObject().containsKey("$addToSet")){
               Document except =  new Document(DataPermissionHelper.FIELD_NAME,new DataPermissionAction(DataPermissionTypeEnums.Role.name(), "658292d19bb96967d3bbc924", new HashSet<>(Arrays.asList("value"))));
                assertEquals(except,resultUpdata.getUpdateObject().get("$addToSet"));
            }
            return null;
        });
        dataPermissionServiceUnderTest.dataAuth(DataPermissionDataTypeEnums.Task,inputDataIds,inputDataPermissionTypeEnums,inputTypeIds,new HashSet<>(Arrays.asList("value")));
        verify(mockMongoOperations).updateMulti(query, update, DataPermissionDataTypeEnums.Task.name());
    }

    @Test
    void testDataAuthActionEmpty() {
        final Set<ObjectId> inputDataIds = new HashSet<>(
                Arrays.asList(MongoUtils.toObjectId("658292d19bb96967d3bbc924")));
        final DataPermissionTypeEnums inputDataPermissionTypeEnums = DataPermissionTypeEnums.Role;
        final Set<String> inputTypeIds = new HashSet<>(Arrays.asList("658292d19bb96967d3bbc924"));
        Query query = Query.query(Criteria.where("_id").in(inputDataIds));
        Update update = Update.fromDocument(new Document("$addToSet", new Document(DataPermissionHelper.FIELD_NAME,new DataPermissionAction(DataPermissionTypeEnums.Role.name(), "658292d19bb96967d3bbc924", new HashSet<>()))));
        dataPermissionServiceUnderTest.dataAuth(DataPermissionDataTypeEnums.Task,inputDataIds,inputDataPermissionTypeEnums,inputTypeIds,new HashSet<>());
        verify(mockMongoOperations,times(0)).updateMulti(query, update, DataPermissionDataTypeEnums.Task.name());
    }

    @Test
    void testDataAuthActionNull() {
        final Set<ObjectId> inputDataIds = new HashSet<>(
                Arrays.asList(MongoUtils.toObjectId("658292d19bb96967d3bbc924")));
        final DataPermissionTypeEnums inputDataPermissionTypeEnums = DataPermissionTypeEnums.Role;
        final Set<String> inputTypeIds = new HashSet<>(Arrays.asList("658292d19bb96967d3bbc924"));
        Query query = Query.query(Criteria.where("_id").in(inputDataIds));
        Update update = Update.fromDocument(new Document("$addToSet", new Document(DataPermissionHelper.FIELD_NAME,new DataPermissionAction(DataPermissionTypeEnums.Role.name(), "658292d19bb96967d3bbc924", null))));
        dataPermissionServiceUnderTest.dataAuth(DataPermissionDataTypeEnums.Task,inputDataIds,inputDataPermissionTypeEnums,inputTypeIds,null);
        verify(mockMongoOperations,times(0)).updateMulti(query, update, DataPermissionDataTypeEnums.Task.name());
    }
}
