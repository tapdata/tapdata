package com.tapdata.tm.dataflowrecord.service;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflowrecord.dto.DataFlowRecordDto;
import com.tapdata.tm.dataflowrecord.entity.DataFlowRecordEntity;
import com.tapdata.tm.dataflowrecord.repository.DataFlowRecordRepository;
import com.tapdata.tm.config.security.UserDetail;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2022/03/04
 * @Description:
 */
@Service
@Slf4j
public class DataFlowRecordService extends BaseService<DataFlowRecordDto, DataFlowRecordEntity, ObjectId, DataFlowRecordRepository> {
    public DataFlowRecordService(@NonNull DataFlowRecordRepository repository) {
        super(repository, DataFlowRecordDto.class, DataFlowRecordEntity.class);
    }

    protected void beforeSave(DataFlowRecordDto dataFlowRecord, UserDetail user) {

    }

    public DataFlowRecordDto saveRecord(DataFlowDto dataFlowDto, String status, UserDetail userDetail){
        DataFlowRecordDto dataFlowRecordDto = new DataFlowRecordDto();
        if (StringUtils.isNotBlank(dataFlowDto.getDataFlowRecordId())){
            dataFlowRecordDto.setId(toObjectId(dataFlowDto.getDataFlowRecordId()));
        }
        dataFlowRecordDto.setDataFlowId(dataFlowDto.getId().toHexString());
        dataFlowRecordDto.setDataFlowStartTime(dataFlowDto.getStartTime());
        dataFlowRecordDto.setDataFlowEndTime(dataFlowDto.getFinishTime());
        dataFlowRecordDto.setDataFlowName(dataFlowRecordDto.getDataFlowName());
        dataFlowRecordDto.setDataFlowStatus(status);
        dataFlowRecordDto.setStartType(dataFlowDto.getStartType());
        return save(dataFlowRecordDto, userDetail);
    }
}