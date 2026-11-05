package com.tapdata.tm.task.service;

import cn.hutool.core.lang.Validator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userGroup.dto.UserGroupDto;
import com.tapdata.tm.userGroup.service.UserGroupService;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 项目导出/导入时搬运任务告警接收人。
 * <p>
 * USER / USER_GROUP 存的是本环境的 id，换环境后 id 不存在。导出时附上稳定键（用户邮箱、组名）和展示用的用户名，
 * 导入时按 id → 稳定键依次映射；映射不到的条目丢弃并返回警告，不中断整批导入。
 * 有条目被丢弃且包里带了有效的 emailReceivers 快照时，用快照补成 EMAIL，避免告警静默丢失。
 */
@Component
@Setter(onMethod_ = {@Autowired})
public class AlarmReceiverTransfer {
    private UserService userService;
    private UserGroupService userGroupService;

    /**
     * 导出前给 USER 附上 email / name(username)，给 USER_GROUP 附上 name。只改导出副本，不落库。
     */
    public void attachExportHints(TaskDto task) {
        if (task == null || CollectionUtils.isEmpty(task.getAlarmReceivers())) {
            return;
        }
        Set<ObjectId> userIds = new HashSet<>();
        Set<ObjectId> groupIds = new HashSet<>();
        for (AlarmReceiver receiver : task.getAlarmReceivers()) {
            ObjectId id = receiver == null ? null : toObjectId(receiver.getId());
            if (id == null) {
                continue;
            }
            if (receiver.getType() == AlarmReceiverType.USER) {
                userIds.add(id);
            } else if (receiver.getType() == AlarmReceiverType.USER_GROUP) {
                groupIds.add(id);
            }
        }
        Map<String, UserDto> users = new HashMap<>();
        if (!userIds.isEmpty()) {
            for (UserDto user : userService.findAll(Query.query(Criteria.where("_id").in(userIds)))) {
                if (user != null && user.getId() != null) {
                    users.put(user.getId().toHexString(), user);
                }
            }
        }
        Map<String, UserGroupDto> groups = new HashMap<>();
        if (!groupIds.isEmpty()) {
            for (UserGroupDto group : userGroupService.findAll(Query.query(Criteria.where("_id").in(groupIds)))) {
                if (group != null && group.getId() != null) {
                    groups.put(group.getId().toHexString(), group);
                }
            }
        }
        for (AlarmReceiver receiver : task.getAlarmReceivers()) {
            if (receiver == null || receiver.getId() == null) {
                continue;
            }
            if (receiver.getType() == AlarmReceiverType.USER && users.containsKey(receiver.getId())) {
                UserDto user = users.get(receiver.getId());
                receiver.setEmail(user.getEmail());
                receiver.setName(user.getUsername());
            } else if (receiver.getType() == AlarmReceiverType.USER_GROUP && groups.containsKey(receiver.getId())) {
                receiver.setName(groups.get(receiver.getId()).getName());
            }
        }
    }

    /**
     * 把导入包里的接收人映射到当前环境，并去掉导出时附带的提示字段，使落库结构与页面写入的一致。
     *
     * @return 需要展示给用户的警告，没有则为空列表
     */
    public List<String> remapForImport(TaskDto task) {
        List<String> warnings = new ArrayList<>();
        if (task == null) {
            return warnings;
        }
        if (task.getEmailReceivers() != null) {
            task.setEmailReceivers(validEmails(task.getEmailReceivers(), warnings));
        }
        if (task.getAlarmReceivers() == null) {
            return warnings;
        }
        List<AlarmReceiver> remapped = new ArrayList<>();
        Set<String> coveredEmails = new HashSet<>();
        boolean dropped = false;
        for (AlarmReceiver receiver : task.getAlarmReceivers()) {
            if (receiver == null || receiver.getType() == null) {
                continue;
            }
            AlarmReceiver mapped = switch (receiver.getType()) {
                case EMAIL -> remapEmail(receiver, warnings);
                case USER -> remapUser(receiver, warnings);
                case USER_GROUP -> remapGroup(receiver, warnings);
            };
            if (mapped == null) {
                dropped = true;
                continue;
            }
            remapped.add(mapped);
            if (mapped.getType() == AlarmReceiverType.EMAIL) {
                coveredEmails.add(mapped.getEmail().toLowerCase(Locale.ROOT));
            } else if (receiver.getType() == AlarmReceiverType.USER && StringUtils.isNotBlank(receiver.getEmail())) {
                coveredEmails.add(receiver.getEmail().trim().toLowerCase(Locale.ROOT));
            }
        }
        if (dropped && CollectionUtils.isNotEmpty(task.getEmailReceivers())) {
            List<String> restored = new ArrayList<>();
            for (String email : task.getEmailReceivers()) {
                if (coveredEmails.add(email.toLowerCase(Locale.ROOT))) {
                    remapped.add(new AlarmReceiver(AlarmReceiverType.EMAIL, null, email));
                    restored.add(email);
                }
            }
            if (!restored.isEmpty()) {
                warnings.add("Alarm receivers restored as plain emails from the exported snapshot: " + String.join(", ", restored));
            }
        }
        task.setAlarmReceivers(remapped);
        return warnings;
    }

    private AlarmReceiver remapEmail(AlarmReceiver receiver, List<String> warnings) {
        String email = StringUtils.trimToEmpty(receiver.getEmail());
        if (!Validator.isEmail(email)) {
            warnings.add("Alarm receiver dropped, illegal email: " + email);
            return null;
        }
        return new AlarmReceiver(AlarmReceiverType.EMAIL, null, email);
    }

    private AlarmReceiver remapUser(AlarmReceiver receiver, List<String> warnings) {
        ObjectId id = toObjectId(receiver.getId());
        if (id != null && userService.findById(id) != null) {
            return new AlarmReceiver(AlarmReceiverType.USER, receiver.getId(), null);
        }
        String email = StringUtils.trimToNull(receiver.getEmail());
        String label = StringUtils.firstNonBlank(receiver.getName(), email, receiver.getId());
        // 只按邮箱匹配：同一地址收件人不变；用户名跨环境可能是另一个人（如 admin），不能用来认人
        UserDto matched = email == null ? null
                : userService.findOne(Query.query(Criteria.where("email").is(email).and("isDeleted").ne(true)));
        if (matched != null && matched.getId() != null) {
            return new AlarmReceiver(AlarmReceiverType.USER, matched.getId().toHexString(), null);
        }
        if (email != null && Validator.isEmail(email)) {
            warnings.add("Alarm receiver user " + label + " not found, kept as plain email");
            return new AlarmReceiver(AlarmReceiverType.EMAIL, null, email);
        }
        warnings.add("Alarm receiver user " + label + " not found, dropped");
        return null;
    }

    private AlarmReceiver remapGroup(AlarmReceiver receiver, List<String> warnings) {
        ObjectId id = toObjectId(receiver.getId());
        if (id != null && userGroupService.findById(id) != null) {
            return new AlarmReceiver(AlarmReceiverType.USER_GROUP, receiver.getId(), null);
        }
        if (StringUtils.isNotBlank(receiver.getName())) {
            UserGroupDto byName = userGroupService.findOne(Query.query(Criteria.where("name").is(receiver.getName())));
            if (byName != null && byName.getId() != null) {
                return new AlarmReceiver(AlarmReceiverType.USER_GROUP, byName.getId().toHexString(), null);
            }
        }
        warnings.add("Alarm receiver group " + StringUtils.firstNonBlank(receiver.getName(), receiver.getId()) + " not found, dropped");
        return null;
    }

    private List<String> validEmails(List<String> emails, List<String> warnings) {
        Set<String> valid = new LinkedHashSet<>();
        for (String raw : emails) {
            String email = StringUtils.trimToEmpty(raw);
            if (email.isEmpty()) {
                continue;
            }
            if (Validator.isEmail(email)) {
                valid.add(email);
            } else {
                warnings.add("Alarm email snapshot dropped, illegal email: " + email);
            }
        }
        return new ArrayList<>(valid);
    }

    private static ObjectId toObjectId(String id) {
        return StringUtils.isNotBlank(id) && ObjectId.isValid(id) ? new ObjectId(id) : null;
    }
}
