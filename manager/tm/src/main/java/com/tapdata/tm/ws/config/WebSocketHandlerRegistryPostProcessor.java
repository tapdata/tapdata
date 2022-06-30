/**
 * @title: WebSocketHandlerProcessor
 * @description:
 * @author lk
 * @date 2021/9/9
 */
package com.tapdata.tm.ws.config;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import static com.tapdata.tm.ws.config.WebSocketConfig.beanNames;
import java.util.Arrays;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

public class WebSocketHandlerRegistryPostProcessor implements BeanFactoryPostProcessor {

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
		beanNames.addAll(Arrays.asList(configurableListableBeanFactory.getBeanNamesForAnnotation(WebSocketMessageHandler.class)));
	}
}
