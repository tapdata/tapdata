package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CloudMailLimitUtilsTest {

    @Test
    void testGetCloudMailLimit() {
        assertThat(CloudMailLimitUtils.getCloudMailLimit()).isEqualTo(CloudMailLimitUtils.CLOUD_MAIL_LIMIT);
    }

    @Test
    void testGetCloudMailLimit_SetEvn() {

        System.setProperty("cloud_mail_limit","5");
        int except = 5;
        assertThat(CloudMailLimitUtils.getCloudMailLimit()).isEqualTo(except);
    }

    @Test
    void testGetCloudMailLimit_SetEvnError() {

        System.setProperty("cloud_mail_limit","test");
        int except = 10;
        assertThat(CloudMailLimitUtils.getCloudMailLimit()).isEqualTo(except);
    }
}
