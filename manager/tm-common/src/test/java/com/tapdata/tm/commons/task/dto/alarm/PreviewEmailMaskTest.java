package com.tapdata.tm.commons.task.dto.alarm;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreviewEmailMaskTest {

    @Test
    void explicitEmailsComeFromEmailEntriesOrLegacyList() {
        List<AlarmReceiver> custom = new ArrayList<>();
        custom.add(new AlarmReceiver(AlarmReceiverType.EMAIL, null, " Ops@Example.com "));
        custom.add(new AlarmReceiver(AlarmReceiverType.USER, "u1", "user@example.com"));
        custom.add(null);
        assertEquals(Set.of("ops@example.com"), PreviewEmailMask.explicitEmails(custom, List.of("snapshot@example.com")));
        assertEquals(Set.of("legacy@example.com"), PreviewEmailMask.explicitEmails(null, List.of("Legacy@example.com", " ")));
        assertTrue(PreviewEmailMask.explicitEmails(null, null).isEmpty());
    }

    @Test
    void derivedEmailsAndEmailShapedUsernamesAreMasked() {
        AlarmReceiverPreview preview = new AlarmReceiverPreview();
        preview.getEmails().add(item("ops@example.com", "邮箱"));
        preview.getEmails().add(item("member@example.com", "研发 / 用户member@example.com"));
        preview.getEmails().add(item("bob@example.com", "用户bob"));
        preview.getEmails().add(null);

        PreviewEmailMask.mask(preview, Set.of("ops@example.com"));

        assertEquals("ops@example.com", preview.getEmails().get(0).getEmail());
        assertEquals(List.of("邮箱"), preview.getEmails().get(0).getSources());
        assertEquals("m***@example.com", preview.getEmails().get(1).getEmail());
        assertEquals(List.of("研发 / 用户m***@example.com"), preview.getEmails().get(1).getSources());
        assertEquals("b***@example.com", preview.getEmails().get(2).getEmail());
        assertEquals(List.of("用户bob"), preview.getEmails().get(2).getSources());
        assertEquals("***", PreviewEmailMask.maskEmail("@nope"));
        PreviewEmailMask.mask(null, Set.of());
    }

    private static AlarmReceiverPreview.PreviewEmail item(String email, String source) {
        AlarmReceiverPreview.PreviewEmail item = new AlarmReceiverPreview.PreviewEmail();
        item.setEmail(email);
        item.setSources(new ArrayList<>(List.of(source)));
        return item;
    }
}
