package com.tapdata.tm.file.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/8/7 11:59
 */
@EqualsAndHashCode(callSuper = true)
@Document("WaitingDeleteFile")
@Data
public class WaitingDeleteFile extends BaseEntity {

    private List<ObjectId> fileIds;
    private String reason;
    private String module;
    private ObjectId resourceId;

}
