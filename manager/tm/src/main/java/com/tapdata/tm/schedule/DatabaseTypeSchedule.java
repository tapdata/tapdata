package com.tapdata.tm.schedule;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.repository.DataSourceDefinitionRepository;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/2 16:48
 */
@Slf4j
@Component
public class DatabaseTypeSchedule {
    @Autowired
    @Setter
    private DataSourceDefinitionRepository dataSourceDefinitionRepository;
    @Autowired
    @Setter
    private FileService fileService;

    /**
     * clean up files that need to be deleted
     */
    @Scheduled(cron = "0 0 2 * * ?")
    @SchedulerLock(name = "DatabaseTypeSchedule.cleanUpForDatabaseTypes", lockAtMostFor = "PT1H", lockAtLeastFor = "PT1H")
    public void cleanUpForDatabaseTypes() {
        log.info("clean up files that need to be deleted");

        Query query = Query.query(Criteria.where("is_deleted").is(true));
        query.fields().include("id", "is_deleted",
                "jarRid", "icon", "messages.zh_CN.doc", "messages.zh_TW.doc", "messages.en_US.doc");
        MongoCollection<Document> collection = dataSourceDefinitionRepository.getMongoOperations()
                .getCollection(PkdSourceService.DATABASE_TYPES_WAITING_DELETED_COLLECTION_NAME);
        FindIterable<DataSourceDefinitionDto> result = collection.find(DataSourceDefinitionDto.class);

        try (MongoCursor<DataSourceDefinitionDto> cursor = result.cursor()) {
            cursor.forEachRemaining(dataSourceDefinitionDto -> {
                List<Boolean> results = new ArrayList<>();
                results.add(deleteFile(MongoUtils.toObjectId(dataSourceDefinitionDto.getJarRid())));
                results.add(deleteFile(MongoUtils.toObjectId(dataSourceDefinitionDto.getIcon())));
                if (dataSourceDefinitionDto.getMessages() != null) {
                    dataSourceDefinitionDto.getMessages().values().forEach(val -> {
                        if (val instanceof Map) {
                            Map<String, Object> map = (Map<String, Object>) val;
                            if (map.containsKey("doc")) {
                                if (map.get("doc") instanceof ObjectId) {
                                    results.add(deleteFile((ObjectId) map.get("doc")));
                                } else if (map.get("doc") instanceof String) {
                                    results.add(deleteFile(MongoUtils.toObjectId((String) map.get("doc"))));
                                }
                            }
                        }
                    });
                }
                if (results.stream().allMatch(Boolean.TRUE::equals)) {
                    Document filter = new Document();
                    filter.put("_id", dataSourceDefinitionDto.getId());
                    collection.deleteOne(filter);
                }
            });
        }
    }

    private Boolean deleteFile(ObjectId id) {
        if (id != null) {
            try {
                fileService.deleteFileById(id);
            } catch (Exception e) {
                log.error("delete file error", e);
                return false;
            }
        }
        return true;
    }
}
