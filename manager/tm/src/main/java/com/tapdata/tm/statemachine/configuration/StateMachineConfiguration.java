/**
 * @title: StateMachineConfiguration
 * @description:
 * @author lk
 * @date 2021/8/6
 */
package com.tapdata.tm.statemachine.configuration;

import com.tapdata.tm.statemachine.StateMachine;
import com.tapdata.tm.statemachine.StateMachineImpl;
import com.tapdata.tm.statemachine.utils.StateMachineProcessorManager;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;

@Configuration
@Slf4j
@Import(StateMachineConfigurationImportBeanRegistrar.class)
public class StateMachineConfiguration implements ApplicationListener<ContextRefreshedEvent> {

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		SpringContextHelper.applicationContext = event.getApplicationContext();
		try {
			StateMachineProcessorManager.init();
		} catch (Exception e) {
			log.error("Build processor failed,message: {}", e.getMessage(), e);
		}
		Map<String, StateMachine> beans = event.getApplicationContext().getBeansOfType(StateMachine.class);
		if (MapUtils.isNotEmpty(beans)){
			beans.values().forEach(StateMachine::initStateTransition);
		}
	}

	@Bean
	public StateMachine initStateMachine(){
		return new StateMachineImpl<>(null);
	}
}
