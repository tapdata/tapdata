package com.tapdata.tm.commons.task.dto.alarm;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlarmReceiverScopeTest {

    @Test
    void onlyUserAndGroupReferencesNeedScopeCheck() {
        List<AlarmReceiver> emailsOnly = new ArrayList<>();
        emailsOnly.add(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "ops@example.com"));
        emailsOnly.add(null);
        assertFalse(AlarmReceiverScope.hasDirectoryReference(emailsOnly));
        assertFalse(AlarmReceiverScope.hasDirectoryReference(null));
        assertTrue(AlarmReceiverScope.hasDirectoryReference(List.of(new AlarmReceiver(AlarmReceiverType.USER_GROUP, "g1", null))));
    }

    @Test
    void referencesOutsideCandidatesAreRejected() {
        AlarmReceiverCandidates candidates = new AlarmReceiverCandidates();
        AlarmReceiverCandidates.CandidateUser user = new AlarmReceiverCandidates.CandidateUser();
        user.setId("u1");
        candidates.getUsers().add(user);
        AlarmReceiverCandidates.CandidateGroup group = new AlarmReceiverCandidates.CandidateGroup();
        group.setId("g1");
        candidates.getGroups().add(group);
        AlarmReceiver otherGroup = new AlarmReceiver(AlarmReceiverType.USER_GROUP, "g2", null);
        AlarmReceiver otherUser = new AlarmReceiver(AlarmReceiverType.USER, "u2", null);
        List<AlarmReceiver> receivers = new ArrayList<>();
        receivers.add(new AlarmReceiver(AlarmReceiverType.USER, "u1", null));
        receivers.add(new AlarmReceiver(AlarmReceiverType.USER_GROUP, "g1", null));
        receivers.add(otherGroup);
        receivers.add(new AlarmReceiver(AlarmReceiverType.EMAIL, null, "any@example.com"));
        receivers.add(otherUser);
        receivers.add(null);

        assertEquals(List.of(otherGroup, otherUser), AlarmReceiverScope.outOfScope(receivers, candidates));
        assertEquals(List.of(otherGroup), AlarmReceiverScope.outOfScope(List.of(otherGroup), null));
        assertTrue(AlarmReceiverScope.outOfScope(null, candidates).isEmpty());
    }
}
