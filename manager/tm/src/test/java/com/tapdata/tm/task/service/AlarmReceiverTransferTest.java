package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverType;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userGroup.dto.UserGroupDto;
import com.tapdata.tm.userGroup.service.UserGroupService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlarmReceiverTransferTest {
    private UserService userService;
    private UserGroupService userGroupService;
    private AlarmReceiverTransfer transfer;

    @BeforeEach
    void setUp() {
        userService = mock(UserService.class);
        userGroupService = mock(UserGroupService.class);
        transfer = new AlarmReceiverTransfer();
        transfer.setUserService(userService);
        transfer.setUserGroupService(userGroupService);
    }

    @Test
    void exportAttachesUserEmailAndGroupName() {
        ObjectId userId = new ObjectId();
        ObjectId groupId = new ObjectId();
        UserDto user = new UserDto();
        user.setId(userId);
        user.setEmail("alice@example.com");
        user.setUsername("alice");
        UserGroupDto group = new UserGroupDto();
        group.setId(groupId);
        group.setName("ops");
        when(userService.findAll(any(Query.class))).thenReturn(List.of(user));
        when(userGroupService.findAll(any(Query.class))).thenReturn(List.of(group));
        TaskDto task = task(
                new AlarmReceiver(AlarmReceiverType.USER, userId.toHexString(), null),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, groupId.toHexString(), null),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "x@example.com"));

        transfer.attachExportHints(task);

        assertEquals(new AlarmReceiver(AlarmReceiverType.USER, userId.toHexString(), "alice@example.com", "alice"), task.getAlarmReceivers().get(0));
        assertEquals(new AlarmReceiver(AlarmReceiverType.USER_GROUP, groupId.toHexString(), null, "ops"), task.getAlarmReceivers().get(1));
        assertEquals(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "x@example.com"), task.getAlarmReceivers().get(2));
    }

    @Test
    void exportWithoutReferencesDoesNotQuery() {
        transfer.attachExportHints(task(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "x@example.com")));
        transfer.attachExportHints(new TaskDto());
        transfer.attachExportHints(null);

        verify(userService, never()).findAll(any(Query.class));
        verify(userGroupService, never()).findAll(any(Query.class));
    }

    @Test
    void sameEnvironmentKeepsIdsAndStripsHints() {
        ObjectId userId = new ObjectId();
        ObjectId groupId = new ObjectId();
        when(userService.findById(userId)).thenReturn(new UserDto());
        when(userGroupService.findById(groupId)).thenReturn(new UserGroupDto());
        TaskDto task = task(
                new AlarmReceiver(AlarmReceiverType.USER, userId.toHexString(), "alice@example.com", "alice"),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, groupId.toHexString(), null, "ops"),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, " x@example.com "));

        List<String> warnings = transfer.remapForImport(task);

        assertTrue(warnings.isEmpty());
        assertEquals(List.of(
                new AlarmReceiver(AlarmReceiverType.USER, userId.toHexString(), null),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, groupId.toHexString(), null),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "x@example.com")), task.getAlarmReceivers());
    }

    @Test
    void crossEnvironmentRemapsByEmailAndGroupName() {
        ObjectId byEmail = new ObjectId();
        ObjectId groupId = new ObjectId();
        UserDto emailMatch = new UserDto();
        emailMatch.setId(byEmail);
        UserGroupDto group = new UserGroupDto();
        group.setId(groupId);
        when(userService.findOne(argThat((Query q) -> q != null && "a@example.com".equals(q.getQueryObject().get("email"))))).thenReturn(emailMatch);
        when(userGroupService.findOne(any(Query.class))).thenReturn(group);
        TaskDto task = task(
                new AlarmReceiver(AlarmReceiverType.USER, new ObjectId().toHexString(), "a@example.com", "alice"),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, new ObjectId().toHexString(), null, "ops"));

        List<String> warnings = transfer.remapForImport(task);

        assertTrue(warnings.isEmpty());
        assertEquals(List.of(
                new AlarmReceiver(AlarmReceiverType.USER, byEmail.toHexString(), null),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, groupId.toHexString(), null)), task.getAlarmReceivers());
    }

    @Test
    void usernameAloneNeverMatchesAnotherAccount() {
        TaskDto task = task(new AlarmReceiver(AlarmReceiverType.USER, new ObjectId().toHexString(), null, "admin"));

        List<String> warnings = transfer.remapForImport(task);

        assertTrue(task.getAlarmReceivers().isEmpty());
        assertEquals(List.of("Alarm receiver user admin not found, dropped"), warnings);
        verify(userService, never()).findOne(any(Query.class));
    }

    @Test
    void unmappedUserDegradesToEmailAndUnmappedGroupFallsBackToSnapshot() {
        TaskDto task = task(
                new AlarmReceiver(AlarmReceiverType.USER, new ObjectId().toHexString(), "alice@example.com", "alice"),
                new AlarmReceiver(AlarmReceiverType.USER_GROUP, new ObjectId().toHexString(), null, "ops"));
        task.setEmailReceivers(List.of("alice@example.com", " member@example.com ", "bad-email"));

        List<String> warnings = transfer.remapForImport(task);

        assertEquals(List.of(
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "alice@example.com"),
                new AlarmReceiver(AlarmReceiverType.EMAIL, null, "member@example.com")), task.getAlarmReceivers());
        assertEquals(List.of("alice@example.com", "member@example.com"), task.getEmailReceivers());
        assertEquals(4, warnings.size());
        assertTrue(warnings.stream().anyMatch(w -> w.contains("bad-email")));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("group ops not found")));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("member@example.com")));
    }

    @Test
    void unmappedWithoutStableKeysIsDroppedWithoutThrowing() {
        String userId = new ObjectId().toHexString();
        List<AlarmReceiver> receivers = new ArrayList<>();
        receivers.add(new AlarmReceiver(AlarmReceiverType.USER, userId, null));
        receivers.add(new AlarmReceiver(AlarmReceiverType.USER_GROUP, "not-an-object-id", null));
        receivers.add(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "nope"));
        receivers.add(null);
        receivers.add(new AlarmReceiver(null, "x", null));
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(receivers);

        List<String> warnings = transfer.remapForImport(task);

        assertTrue(task.getAlarmReceivers().isEmpty());
        assertEquals(3, warnings.size());
        assertTrue(warnings.get(0).contains(userId));
    }

    @Test
    void defaultTaskStaysDefault() {
        TaskDto task = new TaskDto();

        assertTrue(transfer.remapForImport(task).isEmpty());
        assertTrue(transfer.remapForImport(null).isEmpty());
        assertNull(task.getAlarmReceivers());
        assertNull(task.getEmailReceivers());
    }

    private static TaskDto task(AlarmReceiver... receivers) {
        TaskDto task = new TaskDto();
        task.setAlarmReceivers(new ArrayList<>(List.of(receivers)));
        return task;
    }
}
