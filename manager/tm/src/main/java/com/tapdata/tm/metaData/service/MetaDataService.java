package com.tapdata.tm.metaData.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metaData.vo.MetaDataVo;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.utils.BeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class MetaDataService {

    @Autowired
    MetadataInstancesService metadataInstancesService;


    @Autowired
    MetadataInstancesRepository metadataInstancesRepository;

    public Page<MetaDataVo> find(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();
        Map notDeleteMap = new HashMap();
        notDeleteMap.put("$ne", true);
        where.put("is_deleted", notDeleteMap);
        Page<MetadataInstancesDto> page = metadataInstancesService.findMetadataList(filter, userDetail);
        List<MetaDataVo> metaDataVoList = BeanUtil.deepCloneList(page.getItems(), MetaDataVo.class);
        Page<MetaDataVo> result = new Page(page.getTotal(), metaDataVoList);
        return result;
    }
}
