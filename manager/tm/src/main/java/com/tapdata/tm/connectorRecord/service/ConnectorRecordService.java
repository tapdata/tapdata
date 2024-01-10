package com.tapdata.tm.connectorRecord.service;


import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import com.tapdata.tm.connectorRecord.repository.ConnectorRecordRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConnectorRecordService extends BaseService<ConnectorRecordDto, ConnectorRecordEntity, ObjectId, ConnectorRecordRepository> {
    @Autowired
    private MongoTemplate mongoTemplate;

    public ConnectorRecordService(@NonNull ConnectorRecordRepository repository) {
        super(repository, ConnectorRecordDto.class, ConnectorRecordEntity.class);
    }

    @Override
    protected void beforeSave(ConnectorRecordDto dto, UserDetail userDetail) {
        // TODO document why this method is empty
    }

//    public ConnectorRecordEntity uploadConnectorRecord(ConnectorRecordDto connectorRecordDto,UserDetail userDetail){
//        Document document = mongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class,"DatabaseTypes");
//        ConnectorRecordEntity connectorRecord = new ConnectorRecordEntity();
//        BeanUtils.copyProperties(connectorRecordDto,connectorRecord);
//        if(document != null && document.getString("pdkId") != null){
//            connectorRecord.setPdkId(document.getString("pdkId"));
//            return repository.insert(connectorRecord,userDetail);
//        }
//        return null;
//    }
//    public ConnectorRecordEntity queryByConnectionId(String connectionId, UserDetail loginUser) {
//
//        ConnectorRecordEntity connectorRecord = mongoTemplate.findOne(Query.query(Criteria.where("connectionId").is(connectionId)), ConnectorRecordEntity.class);
//        return connectorRecord;
//    }
    public ConnectorRecordEntity queryByCondition(String processId, String pdkHash, UserDetail loginUser) {
        Query query = Query.query(Criteria.where("processId").is(processId).and("pdkHash").is(pdkHash).and("userId").is(loginUser.getUserId()));
        return mongoTemplate.findOne(query, ConnectorRecordEntity.class);
    }
}
