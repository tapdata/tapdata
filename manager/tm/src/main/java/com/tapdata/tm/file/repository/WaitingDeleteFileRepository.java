package com.tapdata.tm.file.repository;

import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.file.entity.WaitingDeleteFile;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/7 12:02
 */
@Repository
public class WaitingDeleteFileRepository extends BaseRepository<WaitingDeleteFile, ObjectId> {

    public WaitingDeleteFileRepository(MongoTemplate mongoOperations) {
        super(WaitingDeleteFile.class, mongoOperations);
    }
}
