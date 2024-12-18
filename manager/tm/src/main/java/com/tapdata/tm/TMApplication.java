package com.tapdata.tm;

import com.tapdata.tm.discovery.service.DefaultDataDirectoryService;
import com.tapdata.tm.ds.service.impl.RepairCreateTimeComponent;
import com.tapdata.tm.listener.StartupListener;
import com.tapdata.tm.report.dto.RunsNumBatch;
import com.tapdata.tm.report.service.UserDataReportService;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.TimeZone;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 6:54 上午
 * @description
 */
@Import(cn.hutool.extra.spring.SpringUtil.class)
@ServletComponentScan("com.tapdata.tm.monitor.servlet")
@SpringBootApplication(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@EnableMongoAuditing
@EnableAsync
@Slf4j
public class TMApplication {

	private static final String TAG = TMApplication.class.getSimpleName();
	private static final Logger pdkLog = org.slf4j.LoggerFactory.getLogger("PDK");

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
		try {
			Class<?> tcmApplicationClass = Class.forName("com.tapdata.manager.TCMApplication");
			String path = System.getenv("TCM_CONF") != null ? System.getenv("TCM_CONF") : "classpath:application-tcm.yml";
			new SpringApplicationBuilder(tcmApplicationClass).properties("spring.config.location="+path)
					.build().run(args);
		}catch (ClassNotFoundException e){
			log.info("No need to start TCM");
		}


		UserDataReportService userDataReportService = applicationContext.getBean(UserDataReportService.class);
		long currentTimeMillis = System.currentTimeMillis();
		RunsNumBatch runsNumBatch = new RunsNumBatch();
		runsNumBatch.setTimestamp(currentTimeMillis);
		userDataReportService.produceData(runsNumBatch);
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
				return msg;
			}
			@Override
			public void debug(String msg) {
				pdkLog.debug(format(msg));
//				System.out.println(msg);
			}

			@Override
			public void info(String msg) {
				pdkLog.info(format(msg));
//					System.out.println(log);
			}

			@Override
			public void warn(String msg) {
				pdkLog.warn(format(msg));
//				System.out.println(msg);
			}

			@Override
			public void error(String msg) {
				pdkLog.error(format(msg));
			}

			@Override
			public void fatal(String msg) {
				pdkLog.error(format(msg));
			}

			@Override
			public void memory(String msg) {
				pdkLog.info(format(msg));
			}
		});

		TapLogger.debug(TAG, "TapRuntime will start");

		buildProperty(userService);
		TapRuntime.getInstance();
		TapLogger.debug(TAG, "TapRuntime initialized");

		new Thread(() -> {
			DefaultDataDirectoryService bean = applicationContext.getBean(DefaultDataDirectoryService.class);
			bean.init();
			LdpService ldpService = applicationContext.getBean(LdpService.class);
			ldpService.generateLdpTaskByOld();

		}).start();

	}

	protected static void buildProperty(UserService userService) {
		String tapdata_proxy_mongodb_uri = CommonUtils.getProperty("tapdata_proxy_mongodb_uri");
		if(tapdata_proxy_mongodb_uri == null)
			CommonUtils.setProperty("tapdata_proxy_mongodb_uri", userService.getMongodbUri());
		CommonUtils.setProperty("tapdata_proxy_server_port", userService.getServerPort());
		CommonUtils.setProperty("tapdata_proxy_mongodb_ssl", userService.isSsl());
		CommonUtils.setProperty("tapdata_proxy_mongodb_caPath", userService.getCaPath());
		CommonUtils.setProperty("tapdata_proxy_mongodb_keyPath", userService.getKeyPath());
	}
}
