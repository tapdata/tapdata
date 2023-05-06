package websocket;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/4/23 下午11:01
 * @description
 */
public class WebSocketTest {

	private Logger logger = LogManager.getLogger(WebSocketClient.class);

	public static void main(String[] args) {

		ConsoleAppender consoleAppender = ConsoleAppender.createDefaultAppenderForLayout(null);

		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig rootLoggerConfig = config.getRootLogger();
		rootLoggerConfig.setLevel(Level.DEBUG);
		AppenderRef ref = AppenderRef.createAppenderRef("rollingFileAppender", Level.DEBUG, null);
		rootLoggerConfig.getAppenderRefs().add(ref);
		ctx.updateLoggers();

		new WebSocketTest().testWS();
	}

	public void testWS() {

		// {"isCloud":true,"jobTags":"-,-","cloud_accessCode":"6172bc16f4aa0e993488d2d40a5f8456","backend_url":"http://192.168.1.182:30104/tm/api/","process_id":"6082d43375548c00103f2649-knudz8hw","user_id":"6062c8fd764d49c7d626f7e3","version":"v1.19.0-543-g14662271f"}

		String wsUrl = "ws://127.0.0.1:8086/tm/ws/agent?agentId={agentId}&access_token={access_token}";
		String agentId = "6082d43375548c00103f2649-knudz8hw";
		String token = "6172bc16f4aa0e993488d2d40a5f8456";
		String message = "{\"type\": \"ping\"}";

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			Thread thread = new Thread(() -> {
				WebSocketClient webSocketClient;
				try {
					webSocketClient = new WebSocketClient(wsUrl, agentId, token);
				} catch (Exception e) {
					logger.error("Create websocket client failed", e);
					return;
				}
				while (true) {
					try {
						if (webSocketClient.isOpen()) {
							webSocketClient.send(message);
						}
					} catch (Exception e) {
						logger.error("Send message failed", e);
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						logger.info("Stop send message");
					}
				}
			});
			thread.start();
			threads.add(thread);
		}

		try {

			BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));
			String line = bufferReader.readLine();

			threads.forEach(Thread::interrupt);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public class WebSocketClient implements WebSocketHandler {

		private final WebSocketSession session;

		public WebSocketClient(String url, String... params) throws ExecutionException, InterruptedException {

			logger.info("Create client for " + url);

			org.springframework.web.socket.client.WebSocketClient client = new StandardWebSocketClient();

			ListenableFuture<WebSocketSession> listenableFuture = client
					.doHandshake(this, url, params);

			session = listenableFuture.get();
			session.setTextMessageSizeLimit(10 * 1024 * 1024);
		}

		public void send(String message) throws IOException {
			if (session.isOpen()) {
				logger.info("Send message: " + message);
				session.sendMessage(new TextMessage(message));
			} else {
				logger.error("Failed send message, the connection is closed.");
			}
		}

		@Override
		public void afterConnectionEstablished(WebSocketSession webSocketSession) throws Exception {
			logger.info("Websocket Connected");
		}

		@Override
		public void handleMessage(WebSocketSession webSocketSession, WebSocketMessage<?> webSocketMessage) throws Exception {
			logger.info("Websocket receive message " + webSocketMessage.getPayload());
		}

		@Override
		public void handleTransportError(WebSocketSession webSocketSession, Throwable throwable) throws Exception {
			logger.error("Websocket transport error", throwable);
		}

		@Override
		public void afterConnectionClosed(WebSocketSession webSocketSession, CloseStatus closeStatus) throws Exception {
			logger.info("Websocket Disconnected");
		}

		public boolean isOpen() {
			return session.isOpen();
		}

		@Override
		public boolean supportsPartialMessages() {
			return false;
		}
	}

}
