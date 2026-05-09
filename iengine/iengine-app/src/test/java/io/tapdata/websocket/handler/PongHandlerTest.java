package io.tapdata.websocket.handler;

import com.tapdata.tm.commons.ping.PingDto;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PongHandlerTest {

	@Test
	void maxWaitDefaultIsBoundedAndShort() {
		// Default WS_PONG_WAIT_MS is 5s. If a future change re-introduces a 30s default,
		// this guard fails so the regression cannot land silently.
		assertTrue(PongHandler.MAX_WAIT_MS <= 10_000L,
			"PongHandler.MAX_WAIT_MS must default to <=10s; was " + PongHandler.MAX_WAIT_MS);
		assertTrue(PongHandler.MAX_WAIT_MS >= 100L,
			"PongHandler.MAX_WAIT_MS must be >=100ms; was " + PongHandler.MAX_WAIT_MS);
	}

	@Test
	void handleResponseReturnsTrueWhenPongArrives() {
		PongHandler handler = new PongHandler();
		String pingId = "ping-fast-" + System.nanoTime();

		Thread responder = new Thread(() -> {
			try {
				Thread.sleep(50L);
			} catch (InterruptedException ignored) {
				Thread.currentThread().interrupt();
			}
			Map<String, Object> pong = new HashMap<>();
			pong.put("pingId", pingId);
			pong.put(PingDto.PING_RESULT, "ok");
			handler.handle(pong);
		});
		responder.setDaemon(true);
		responder.start();

		long start = System.currentTimeMillis();
		AtomicReference<Map<String, Object>> received = new AtomicReference<>();
		boolean responded = PongHandler.handleResponse(pingId, received::set);
		long elapsed = System.currentTimeMillis() - start;

		assertTrue(responded, "Expected pong to be received");
		assertNotNull(received.get(), "Consumer must observe the pong payload");
		assertEquals(pingId, received.get().get("pingId"));
		assertTrue(elapsed < PongHandler.MAX_WAIT_MS,
			"handleResponse should return as soon as pong arrives, elapsed=" + elapsed);
	}

	@Test
	void handleResponseReturnsFalseAfterTimeoutForUnknownPingId() {
		String unknownId = "ping-unknown-" + System.nanoTime();
		long start = System.currentTimeMillis();
		boolean responded = PongHandler.handleResponse(unknownId, m -> {});
		long elapsed = System.currentTimeMillis() - start;

		assertFalse(responded, "Unknown pingId must not flag success");
		assertTrue(elapsed >= PongHandler.MAX_WAIT_MS - 200L,
			"Timeout fired too early, elapsed=" + elapsed + " MAX_WAIT_MS=" + PongHandler.MAX_WAIT_MS);
		assertTrue(elapsed <= PongHandler.MAX_WAIT_MS + 2_000L,
			"Timeout fired too late, elapsed=" + elapsed + " MAX_WAIT_MS=" + PongHandler.MAX_WAIT_MS);
	}
}
