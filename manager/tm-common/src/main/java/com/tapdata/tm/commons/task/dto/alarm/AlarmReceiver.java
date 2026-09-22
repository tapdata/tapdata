package com.tapdata.tm.commons.task.dto.alarm;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;

/**
 * 任务上保存的一条告警接收对象。这里存的是引用，不是发送时的解析结果。
 * 任务启动时会随 TaskDto 进入 Hazelcast DAG，必须可序列化。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlarmReceiver implements Serializable {
    private AlarmReceiverType type;
    /** USER 为 userId，USER_GROUP 为 userGroupId，EMAIL 为空。必须写成 id，不能被当成子文档 _id。 */
    @Field("id")
    private String id;
    /** EMAIL 为明文地址。USER 上的值只是展示快照，发送时不采用 */
    private String email;
}
