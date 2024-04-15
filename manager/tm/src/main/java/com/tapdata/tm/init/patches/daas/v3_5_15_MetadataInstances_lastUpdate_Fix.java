package com.tapdata.tm.init.patches.daas;

import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@PatchAnnotation(appType = AppType.DAAS, version = "3.5-15")
public class v3_5_15_MetadataInstances_lastUpdate_Fix extends AbsPatch {
    public static final String TAG = v3_5_15_MetadataInstances_lastUpdate_Fix.class.getSimpleName();
    public v3_5_15_MetadataInstances_lastUpdate_Fix(PatchType type, PatchVersion version) {
        super(type, version);
    }

    @Override
    public void run() {
        MetadataInstancesRepository metadataInstancesRepository = SpringContextHelper.getBean(MetadataInstancesRepository.class);
        Query query = new Query(Criteria.where("meta_type").is("database"));
        query.fields().include("source._id","lastUpdate","qualified_name");
        List<MetadataInstancesEntity> metadataInstancesEntityList= metadataInstancesRepository.findAll(query);
        Map<String,Long> lastUpdateMap = new HashMap<>();
        for(MetadataInstancesEntity metadataInstances : metadataInstancesEntityList){
            Query queryLastUpdateDate = Query.query(
                    Criteria.where("source._id").is(metadataInstances.getSource().get_id()).and("meta_type").is("table")
                            .and("sourceType").is("SOURCE").and("lastUpdate").ne(null));
            queryLastUpdateDate.fields().include("databaseId","lastUpdate");
            Optional<MetadataInstancesEntity> metadataInstancesEntityOptional = metadataInstancesRepository.findOne(queryLastUpdateDate);
            if(metadataInstancesEntityOptional.isPresent()){
                metadataInstancesRepository.update(new Query(Criteria.where("qualified_name").is(metadataInstances.getQualifiedName())), Update.update("lastUpdate",metadataInstancesEntityOptional.get().getLastUpdate()));
                lastUpdateMap.put(metadataInstancesEntityOptional.get().getDatabaseId(),metadataInstancesEntityOptional.get().getLastUpdate());
            }else{
                Long time  = System.currentTimeMillis();
                metadataInstancesRepository.update(new Query(Criteria.where("qualified_name").is(metadataInstances.getQualifiedName())), Update.update("lastUpdate",time));
            }
        }
        Query addQuery = new Query(Criteria.where("sourceType").is("SOURCE").and("meta_type").is("table").and("lastUpdate").is(null));
        query.fields().include("databaseId","lastUpdate","qualified_name");
        List<MetadataInstancesEntity> addMetadataInstancesEntityList= metadataInstancesRepository.findAll(addQuery);
        for(MetadataInstancesEntity metadataInstancesEntity :addMetadataInstancesEntityList){
            metadataInstancesRepository.update(new Query(Criteria.where("qualified_name").is(metadataInstancesEntity.getQualifiedName())),
                    Update.update("lastUpdate",lastUpdateMap.get(metadataInstancesEntity.getDatabaseId()) == null ? System.currentTimeMillis() : lastUpdateMap.get(metadataInstancesEntity.getDatabaseId()) ));
        }

    }
}
