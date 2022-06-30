package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.BaseJunit;
import lombok.extern.slf4j.Slf4j;
import org.jasypt.encryption.StringEncryptor;
import org.jasypt.util.text.BasicTextEncryptor;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class EncryptionTest extends BaseJunit {
    @Autowired
    StringEncryptor stringEncryptor;

    @Test
   public void databaseEncryptionTest() {
        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        //加密所需的salt(盐)
        textEncryptor.setPassword("myApplicationSalt");
        //要加密的数据（数据库的用户名或密码）
    }

}
