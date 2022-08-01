package com.tapdata.tm.oauth;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/21 下午4:26
 */

public class TestPasswordEncoder {

    @Test
    public void testPasswordEncoder() {

        BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

        String passwordSecurity = "$2a$10$VmrRIazvMqgNc.S8kSx5Ju0GOZnY3lh.vS7VLgNBJ52t4TVrqb77K";

        String passwordEncoderStr = passwordEncoder.encode("admin");

        Assert.assertTrue(passwordEncoder.matches("admin", passwordEncoderStr));
        Assert.assertTrue(passwordEncoder.matches("admin", passwordSecurity));


    }

}
