package com.tapdata.tm.Settings.repository;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.TMApplication;
import com.tapdata.tm.user.repository.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@Slf4j
class UserRepositoryTest extends BaseJunit {

    @Autowired
    UserRepository userRepository;

    @Test
    void findByCategory() {
//        System.out.println(settingsRepository.findByCategory("Worker"));
    }

    @Test
    void findByCategoryAndKey() {
        printResult(userRepository.findById("6050575762ed301e55add7fb"));
    }

}