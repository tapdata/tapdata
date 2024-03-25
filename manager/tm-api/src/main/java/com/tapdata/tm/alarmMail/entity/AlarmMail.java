package com.tapdata.tm.alarmMail.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import lombok.*;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Settings_Alarm_Mail")
public class AlarmMail extends BaseEntity {
    private NotifyEnum type;
    private List<String> emailAddressList;

}
