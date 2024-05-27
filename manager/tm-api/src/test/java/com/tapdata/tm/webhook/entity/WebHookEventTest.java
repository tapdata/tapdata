package com.tapdata.tm.webhook.entity;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WebHookEventTest {
    @Test
    void testAll() {
        WebHookEvent of = WebHookEvent.of();
        of.withTitle("title")
                .withContent("content")
                .withMetric("")
                .withUserId(null)
                .withEvent("")
                .withType("");
        Assertions.assertEquals("", of.getType());
        Assertions.assertEquals("title", of.getTitle());
        Assertions.assertEquals("content", of.getContent());
        Assertions.assertNull(of.getUserId());
        Assertions.assertEquals("", of.getMetric());
        Assertions.assertEquals("", of.getEvent());
        of.setContent("c");
        of.setTitle("t");
        Assertions.assertEquals("t", of.getTitle());
        Assertions.assertEquals("c", of.getContent());
    }
}