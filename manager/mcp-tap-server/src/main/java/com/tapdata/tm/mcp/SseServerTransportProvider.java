/*
 * Copyright 2024-2024 the original author or authors.
 */

package com.tapdata.tm.mcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.ThreadLocalUtils;
import io.modelcontextprotocol.spec.*;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import org.springframework.web.servlet.function.ServerResponse.SseBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.tapdata.tm.mcp.McpConfig.TOKEN;
import static com.tapdata.tm.mcp.McpConfig.USER_ID;

/**
 * Server-side implementation of the Model Context Protocol (MCP) transport layer using
 * HTTP with Server-Sent Events (SSE) through Spring WebMVC. This implementation provides
 * a bridge between synchronous WebMVC operations and reactive programming patterns to
 * maintain compatibility with the reactive transport interface.
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Implements bidirectional communication using HTTP POST for client-to-server
 * messages and SSE for server-to-client messages</li>
 * <li>Manages client sessions with unique IDs for reliable message delivery</li>
 * <li>Supports graceful shutdown with proper session cleanup</li>
 * <li>Provides JSON-RPC message handling through configured endpoints</li>
 * <li>Includes built-in error handling and logging</li>
 * </ul>
 *
 * <p>
 * The transport operates on two main endpoints:
 * <ul>
 * <li>{@code /sse} - The SSE endpoint where clients establish their event stream
 * connection</li>
 * <li>A configurable message endpoint where clients send their JSON-RPC messages via HTTP
 * POST</li>
 * </ul>
 *
 * <p>
 * This implementation uses {@link ConcurrentHashMap} to safely manage multiple client
 * sessions in a thread-safe manner. Each client session is assigned a unique ID and
 * maintains its own SSE connection.
 *
 * @author Christian Tzolov
 * @author Alexandros Pappas
 * @see McpServerTransportProvider
 * @see RouterFunction
 */
public class SseServerTransportProvider implements McpServerTransportProvider, SessionAttribute {

	private static final Logger logger = LoggerFactory.getLogger(SseServerTransportProvider.class);

	/**
	 * Event type for JSON-RPC messages sent through the SSE connection.
	 */
	public static final String MESSAGE_EVENT_TYPE = "message";

	/**
	 * Event type for sending the message endpoint URI to clients.
	 */
	public static final String ENDPOINT_EVENT_TYPE = "endpoint";

	/**
	 * Default SSE endpoint path as specified by the MCP transport specification.
	 */
	public static final String DEFAULT_SSE_ENDPOINT = "/sse";

	private final ObjectMapper objectMapper;

	private final String messageEndpoint;

	private final String sseEndpoint;

	private final RouterFunction<ServerResponse> routerFunction;
	private final UserLogService userLogService;

	private McpServerSession.Factory sessionFactory;

	/**
	 * Map of active client sessions, keyed by session ID.
	 */
	private final ConcurrentHashMap<String, McpServerSession> sessions = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, Map<String, Object>> sessionAttributes = new ConcurrentHashMap<>();

	/**
	 * Flag indicating if the transport is shutting down.
	 */
	private volatile boolean isClosing = false;

	/**
	 * Constructs a new WebMvcSseServerTransportProvider instance.
	 * @param objectMapper The ObjectMapper to use for JSON serialization/deserialization
	 * of messages.
	 * @param messageEndpoint The endpoint URI where clients should send their JSON-RPC
	 * messages via HTTP POST. This endpoint will be communicated to clients through the
	 * SSE connection's initial endpoint event.
	 * @param sseEndpoint The endpoint URI where clients establish their SSE connections.
	 * @throws IllegalArgumentException if any parameter is null
	 */
	public SseServerTransportProvider(ObjectMapper objectMapper, String messageEndpoint, String sseEndpoint, UserLogService userLogService) {
		Assert.notNull(objectMapper, "ObjectMapper must not be null");
		Assert.notNull(messageEndpoint, "Message endpoint must not be null");
		Assert.notNull(sseEndpoint, "SSE endpoint must not be null");

		this.objectMapper = objectMapper;
		this.messageEndpoint = messageEndpoint;
		this.sseEndpoint = sseEndpoint;
		this.userLogService = userLogService;
		this.routerFunction = RouterFunctions.route()
			.GET(this.sseEndpoint, this::handleSseConnection)
			.POST(this.messageEndpoint, this::handleMessage)
			.build();
	}

	@Override
	public void setSessionFactory(McpServerSession.Factory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Broadcasts a notification to all connected clients through their SSE connections.
	 * The message is serialized to JSON and sent as an SSE event with type "message". If
	 * any errors occur during sending to a particular client, they are logged but don't
	 * prevent sending to other clients.
	 * @param method The method name for the notification
	 * @param params The parameters for the notification
	 * @return A Mono that completes when the broadcast attempt is finished
	 */
	@Override
	public Mono<Void> notifyClients(String method, Object params) {
		if (sessions.isEmpty()) {
			logger.debug("No active sessions to broadcast message to");
			return Mono.empty();
		}

		logger.debug("Attempting to broadcast message to {} active sessions", sessions.size());

		return Flux.fromIterable(sessions.values())
				.flatMap(session -> session.sendNotification(method, params)
						.doOnError(
								e -> logger.error("Failed to send message to session {}: {}", session.getId(), e.getMessage())))
				.then();
	}

	/**
	 * Initiates a graceful shutdown of the transport. This method:
	 * <ul>
	 * <li>Sets the closing flag to prevent new connections</li>
	 * <li>Closes all active SSE connections</li>
	 * <li>Removes all session records</li>
	 * </ul>
	 * @return A Mono that completes when all cleanup operations are finished
	 */
	@Override
	public Mono<Void> closeGracefully() {
		return Flux.fromIterable(sessions.values()).doFirst(() -> {
			this.isClosing = true;
			logger.debug("Initiating graceful shutdown with {} active sessions", sessions.size());
		})
			.flatMap(McpServerSession::closeGracefully)
			.then()
			.doOnSuccess(v -> logger.debug("Graceful shutdown completed"));
	}

	/**
	 * Returns the RouterFunction that defines the HTTP endpoints for this transport. The
	 * router function handles two endpoints:
	 * <ul>
	 * <li>GET /sse - For establishing SSE connections</li>
	 * <li>POST [messageEndpoint] - For receiving JSON-RPC messages from clients</li>
	 * </ul>
	 * @return The configured RouterFunction for handling HTTP requests
	 */
	public RouterFunction<ServerResponse> getRouterFunction() {
		return this.routerFunction;
	}

	/**
	 * Handles new SSE connection requests from clients by creating a new session and
	 * establishing an SSE connection. This method:
	 * <ul>
	 * <li>Generates a unique session ID</li>
	 * <li>Creates a new session with a WebMvcMcpSessionTransport</li>
	 * <li>Sends an initial endpoint event to inform the client where to send
	 * messages</li>
	 * <li>Maintains the session in the sessions map</li>
	 * </ul>
	 * @param request The incoming server request
	 * @return A ServerResponse configured for SSE communication, or an error response if
	 * the server is shutting down or the connection fails
	 */
	private ServerResponse handleSseConnection(ServerRequest request) {
		if (this.isClosing) {
			return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).body("Server is shutting down");
		}

		String sessionId = UUID.randomUUID().toString();
		logger.debug("Creating new SSE connection for session: {}", sessionId);

		// Send initial endpoint event
		try {
			return ServerResponse.sse(sseBuilder -> {
				sseBuilder.onComplete(() -> {
					logger.debug("SSE connection completed for session: {}", sessionId);
					sessions.remove(sessionId);
					sessionAttributes.remove(sessionId);
				});
				sseBuilder.onTimeout(() -> {
					logger.debug("SSE connection timed out for session: {}", sessionId);
					sessions.remove(sessionId);
					sessionAttributes.remove(sessionId);
				});

				WebMvcMcpSessionTransport sessionTransport = new WebMvcMcpSessionTransport(sessionId, sseBuilder);
				McpServerSession session = sessionFactory.create(sessionTransport);
				this.sessions.put(sessionId, session);

				Map<String,Object> sessionAttribute = new ConcurrentHashMap<>();
				sessionAttribute.put("sessionId", session.getId());
				Optional.ofNullable(request.session().getAttribute(TOKEN))
						.ifPresent(accessToken -> sessionAttribute.put(TOKEN, accessToken));
				Optional.ofNullable(request.session().getAttribute(USER_ID))
						.ifPresent(userId -> sessionAttribute.put(USER_ID, userId));
				Optional.ofNullable(request.session().getAttribute("params"))
						.ifPresent(accessToken -> sessionAttribute.put("params", accessToken));
				this.sessionAttributes.put(sessionId, sessionAttribute);

				Object userId = request.session().getAttribute(USER_ID);
				if (userId != null) {
					userLogService.addUserLog(Modular.MCP, Operation.CONNECTED, userId.toString(), null, null);
				}

				try {
					sseBuilder.id(sessionId)
						.event(ENDPOINT_EVENT_TYPE)
						.data(messageEndpoint + "?sessionId=" + sessionId);
				}
				catch (Exception e) {
					logger.error("Failed to send initial endpoint event: {}", e.getMessage());
					sseBuilder.error(e);
				}
			}, Duration.of(24, ChronoUnit.HOURS));
		}
		catch (Exception e) {
			logger.error("Failed to send initial endpoint event to session {}: {}", sessionId, e.getMessage());
			sessions.remove(sessionId);
			sessionAttributes.remove(sessionId);
			return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}

	/**
	 * Handles incoming JSON-RPC messages from clients. This method:
	 * <ul>
	 * <li>Deserializes the request body into a JSON-RPC message</li>
	 * <li>Processes the message through the session's handle method</li>
	 * <li>Returns appropriate HTTP responses based on the processing result</li>
	 * </ul>
	 * @param request The incoming server request containing the JSON-RPC message
	 * @return A ServerResponse indicating success (200 OK) or appropriate error status
	 * with error details in case of failures
	 */
	private ServerResponse handleMessage(ServerRequest request) {
		if (this.isClosing) {
			return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).body("Server is shutting down");
		}

		String sessionId = request.param("sessionId").orElse(null);
		if (sessionId == null) {
			return ServerResponse.badRequest().body(new McpError("Session ID missing in message endpoint"));
		}

		McpServerSession session = sessions.get(sessionId);

		if (session == null) {
			return ServerResponse.status(HttpStatus.NOT_FOUND).body(new McpError("Session not found: " + sessionId));
		}

		try {
			String body = request.body(String.class);
			McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(objectMapper, body);

			Object userId = getAttribute(sessionId, USER_ID);
			if (userId != null) {
				userLogService.addUserLog(Modular.MCP, Operation.READ, userId.toString(), null, body);
			}

			// Process the message through the session's handle method
			session.handle(message).block(); // Block for WebMVC compatibility

			return ServerResponse.ok().build();
		}
		catch (IllegalArgumentException | IOException e) {
			logger.error("Failed to deserialize message: {}", e.getMessage());
			return ServerResponse.badRequest().body(new McpError("Invalid message format"));
		}
		catch (Exception e) {
			logger.error("Error handling message: {}", e.getMessage());
			return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new McpError(e.getMessage()));
		}
	}

	private Map<String, Object> getSessionAttributesOrNull(String sessionId) {
		if (sessionId == null) return null;
		if (sessionAttributes.containsKey(sessionId))
			return sessionAttributes.get(sessionId);
		return sessionAttributes.values().stream().filter(c -> sessionId.equals(c.get("sessionId"))).findFirst().orElse(null);
	}
	public Map<String, Object> getSessionAttributes(String sessionId) {
		return Optional.ofNullable(getSessionAttributesOrNull(sessionId)).orElse(Collections.emptyMap());
	}

	public Object getAttribute(String sessionId, String key) {
		return getSessionAttributes(sessionId).get(key);
	}

	public Object setAttribute(String sessionId, String key, Object val) {
		return Optional.ofNullable(getSessionAttributesOrNull(sessionId)).map(attrs -> attrs.put(key, val)).orElse(null);
	}

	/**
	 * Implementation of McpServerTransport for WebMVC SSE sessions. This class handles
	 * the transport-level communication for a specific client session.
	 */
	private class WebMvcMcpSessionTransport implements McpServerTransport {

		private final String sessionId;

		private final SseBuilder sseBuilder;

		/**
		 * Creates a new session transport with the specified ID and SSE builder.
		 * @param sessionId The unique identifier for this session
		 * @param sseBuilder The SSE builder for sending server events to the client
		 */
		WebMvcMcpSessionTransport(String sessionId, SseBuilder sseBuilder) {
			this.sessionId = sessionId;
			this.sseBuilder = sseBuilder;
			logger.debug("Session transport {} initialized with SSE builder", sessionId);
		}

		/**
		 * Sends a JSON-RPC message to the client through the SSE connection.
		 * @param message The JSON-RPC message to send
		 * @return A Mono that completes when the message has been sent
		 */
		@Override
		public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {
			return Mono.fromRunnable(() -> {
				try {
					String jsonText = objectMapper.writeValueAsString(message);
					sseBuilder.id(sessionId).event(MESSAGE_EVENT_TYPE).data(jsonText);
					logger.debug("Message sent to session {}", sessionId);
				}
				catch (Exception e) {
					logger.error("Failed to send message to session {}: {}", sessionId, e.getMessage());
					sseBuilder.error(e);
				}
			});
		}

		/**
		 * Converts data from one type to another using the configured ObjectMapper.
		 * @param data The source data object to convert
		 * @param typeRef The target type reference
		 * @return The converted object of type T
		 * @param <T> The target type
		 */
		@Override
		public <T> T unmarshalFrom(Object data, TypeReference<T> typeRef) {
			return objectMapper.convertValue(data, typeRef);
		}

		/**
		 * Initiates a graceful shutdown of the transport.
		 * @return A Mono that completes when the shutdown is complete
		 */
		@Override
		public Mono<Void> closeGracefully() {
			return Mono.fromRunnable(() -> {
				logger.debug("Closing session transport: {}", sessionId);
				try {
					sseBuilder.complete();
					logger.debug("Successfully completed SSE builder for session {}", sessionId);
				}
				catch (Exception e) {
					logger.warn("Failed to complete SSE builder for session {}: {}", sessionId, e.getMessage());
				}
			});
		}

		/**
		 * Closes the transport immediately.
		 */
		@Override
		public void close() {
			try {
				sseBuilder.complete();
				logger.debug("Successfully completed SSE builder for session {}", sessionId);
			}
			catch (Exception e) {
				logger.warn("Failed to complete SSE builder for session {}: {}", sessionId, e.getMessage());
			}
		}

	}

}
