/**
 * @title: WebSocketConfig
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.ws.config;

import com.tapdata.tm.ws.annotation.WebSocketMessageHandler;
import com.tapdata.tm.ws.endpoint.WebSocketClusterServer;
import com.tapdata.tm.ws.endpoint.WebSocketManager;
import com.tapdata.tm.ws.endpoint.WebSocketServer;
import com.tapdata.tm.ws.handler.WebSocketHandler;
import org.apache.tomcat.websocket.server.Constants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.ServletContext;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
@Import({WebSocketHandlerRegistryPostProcessor.class, WebSocketHandlerImportBeanRegistrar.class})
public class WebSocketConfig implements WebSocketConfigurer, ServletContextInitializer, ApplicationListener<ContextRefreshedEvent> {

	static final List<String> beanNames = new ArrayList<>();

	private final WebSocketServer webSocket;

	private final WebSocketClusterServer webSocketClusterServer;

	public WebSocketConfig(WebSocketServer webSocket, WebSocketClusterServer webSocketClusterServer) {
		this.webSocket = webSocket;
		this.webSocketClusterServer = webSocketClusterServer;
	}

	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		registry
				.addHandler(webSocket, "/ws/agent")
				.addHandler(webSocketClusterServer, "/ws/cluster/")
				.setAllowedOrigins("*");
	}

	@Override
	public void onStartup(ServletContext servletContext) {
		servletContext.setInitParameter(Constants.BINARY_BUFFER_SIZE_SERVLET_CONTEXT_INIT_PARAM, String.valueOf(8 * 1024 * 10));
		servletContext.setInitParameter(Constants.TEXT_BUFFER_SIZE_SERVLET_CONTEXT_INIT_PARAM, String.valueOf(8 * 1024 * 10));
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
		ApplicationContext applicationContext = contextRefreshedEvent.getApplicationContext();
		for (String beanName : beanNames) {
			Object bean = applicationContext.getBean(beanName);
			Class<?> aClass = bean.getClass();
			if (bean instanceof WebSocketHandler && aClass.isAnnotationPresent(WebSocketMessageHandler.class)){
				WebSocketMessageHandler webSocketMessageHandler = aClass.getDeclaredAnnotation(WebSocketMessageHandler.class);
				Arrays.stream(webSocketMessageHandler.type())
						.forEach(type -> WebSocketManager.addHandler(type.getType(), (WebSocketHandler) bean));
			}
		}

	}

}
