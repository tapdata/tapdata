package com.tapdata.tm;

import com.tapdata.tm.ds.service.impl.RepairCreateTimeComponent;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.listener.StartupListener;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.TimeZone;

import static io.tapdata.pdk.core.utils.CommonUtils.dateString;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 6:54 上午
 * @description
 */
@Import(cn.hutool.extra.spring.SpringUtil.class)
@ServletComponentScan("com.tapdata.tm.monitor.servlet")
@SpringBootApplication
@EnableMongoAuditing
@EnableMongoRepositories
@EnableAsync
@Slf4j
public class TMApplication {

	private static final String TAG = TMApplication.class.getSimpleName();


	/**
	 * 切记：在这个面里面加东西，一定要在企业版的启动类里面一起加，不然，企业版的启动类缺少这里的执行步骤，会让你怀疑人生
	 * 切记：在这个面里面加东西，一定要在企业版的启动类里面一起加，不然，企业版的启动类缺少这里的执行步骤，会让你怀疑人生
	 * 切记：在这个面里面加东西，一定要在企业版的启动类里面一起加，不然，企业版的启动类缺少这里的执行步骤，会让你怀疑人生
	 * @param args
	 */
	public static void main(String[] args) {
		CommonUtils.setProperty("tap_verbose", "true");

		ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder(TMApplication.class)
				.listeners(new StartupListener())
				.build().run(args);
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

		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
		TapLogger.setLogListener(new TapLogger.LogListener() {
			String format(String msg) {
				return "PDK - " + dateString() + " " + Thread.currentThread().getName() + ": " + msg;
			}
			@Override
			public void debug(String msg) {
				log.debug(format(msg));
//				System.out.println(msg);
			}

			@Override
			public void info(String msg) {
				log.info(format(msg));
//					System.out.println(log);
			}

			@Override
			public void warn(String msg) {
				log.warn(format(msg));
//				System.out.println(msg);
			}

			@Override
			public void error(String msg) {
				log.error(format(msg));
			}

			@Override
			public void fatal(String msg) {
				log.error(format(msg));
			}

			@Override
			public void memory(String msg) {
				log.info(format(msg));
			}
		});

		TapLogger.debug(TAG, "TapRuntime will start");

		CommonUtils.setProperty("tapdata_proxy_mongodb_uri", userService.getMongodbUri());
		CommonUtils.setProperty("tapdata_proxy_server_port", userService.getServerPort());
		TapRuntime.getInstance();
		TapLogger.debug(TAG, "TapRuntime initialized");

		new Thread(() -> {
			DefaultDataDirectoryService bean = applicationContext.getBean(DefaultDataDirectoryService.class);
			bean.init();
		}).start();

	}
}
