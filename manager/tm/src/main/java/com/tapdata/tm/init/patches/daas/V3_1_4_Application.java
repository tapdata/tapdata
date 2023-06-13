package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.utils.SpringContextHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.stream.Collectors;

@PatchAnnotation(appType = AppType.DAAS, version = "3.1-4")
public class V3_1_4_Application extends AbsPatch {

    private static final Logger logger = LogManager.getLogger(V3_1_4_Application.class);
    public V3_1_4_Application(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        logger.info("Application java patch: {}...", getClass().getName());
        ApplicationRepository applicationRepository = SpringContextHelper.getBean(ApplicationRepository.class);
        RoleRepository roleRepository = SpringContextHelper.getBean(RoleRepository.class);
        Map<String, ObjectId> roleMap = roleRepository.findAll(Query.query(Criteria.where("_id").ne(null))).stream()
                .collect(Collectors.toMap(RoleEntity::getName,RoleEntity::getId,(key1, key2) -> key2));
        List<ApplicationEntity> applicationEntityList = applicationRepository.findAll(Query.query(Criteria.where("_id").ne(null)));
        applicationEntityList.forEach(applicationEntity -> {
            List scopesList = new ArrayList<>();
            applicationEntity.getScopes().forEach(s ->{
                if(roleMap.containsKey(s)){
                    scopesList.add(roleMap.get(s.toString()).toString());
                }
            });
            applicationEntity.setScopes(scopesList);
            if(CollectionUtils.isNotEmpty(scopesList)){
                applicationRepository.update(Query.query(Criteria.where("_id").is(applicationEntity.getId())),applicationEntity);
            }
        });
    }
}
