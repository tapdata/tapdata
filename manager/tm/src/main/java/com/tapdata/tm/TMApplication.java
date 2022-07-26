package com.tapdata.tm;

import com.tapdata.tm.ds.service.impl.RepairCreateTimeComponent;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 6:54 上午
 * @description
 */
@ServletComponentScan("com.tapdata.tm.monitor.servlet")
@SpringBootApplication
@EnableMongoAuditing
@EnableMongoRepositories
@EnableAsync
@Slf4j
public class TMApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext applicationContext = SpringApplication.run(TMApplication.class, args);
		SpringContextHelper.applicationContext = applicationContext;

		new Thread(()->{
			RepairCreateTimeComponent repairCreateTimeComponent = applicationContext.getBean("repairCreateTimeComponent", RepairCreateTimeComponent.class);
			repairCreateTimeComponent.repair();
		}).start();

		UserService userService = applicationContext.getBean(UserService.class);
		Query query = new Query(Criteria.where("email").is("admin@admin.com"));
		query.fields().include("accessCode");

		UserDto userDto = userService.findOne(query);
		if (userDto != null) {
			log.info("admin access code is {}", userDto.getAccessCode());
		}

	}
}
