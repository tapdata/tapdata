package imclient;

import com.google.common.collect.Lists;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.data.IncomingData;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.modules.api.proxy.data.NodeSubscribeInfo;
import io.tapdata.modules.api.proxy.data.TestItem;
import io.tapdata.pdk.core.runtime.TapRuntime;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.wsclient.modules.imclient.IMClient;
import io.tapdata.wsclient.modules.imclient.IMClientBuilder;
import io.tapdata.wsclient.modules.imclient.impls.websocket.ChannelStatus;
import io.tapdata.wsclient.utils.EventManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class IMClientTest extends AsyncTestBase{
//	@BeforeEach
//	public void before() {
//		CommonUtils.setProperty("TAPDATA_MONGO_URI", "mongodb://127.0.0.1:27017/tapdata?authSource=admin");
//		TapRuntime.getInstance();
//	}
	private IMClient imClient;
	@Test
	@Disabled
	public void test() throws Throwable {
		TapRuntime.getInstance();

		imClient = new IMClientBuilder()
				.withBaseUrl(Lists.newArrayList("http://localhost:3000/api/proxy?access_token=ed305eb7ec5c4c85b3095d0af58341c5bf49101b3608485fb8db4ab8d884cf06"))
				.withService("test")
				.withPrefix("e")
				.withClientId("aplombtest")
				.withTerminal(1)
				.withToken("95c31adb8a1e4d9194ba19114919eed324010045a48749fc8f2ac4c9ff2dac8b")
				.build();
		imClient.start();
		EventManager eventManager = EventManager.getInstance();
		eventManager.registerEventListener(imClient.getPrefix() + ".status", this::handleStatus);
		//prefix + "." + data.getClass().getSimpleName() + "." + data.getContentType()
		eventManager.registerEventListener(imClient.getPrefix() + "." + OutgoingData.class.getSimpleName() + "." + NewDataReceived.class.getSimpleName(), this::handleNewDataReceived);

		waitCompleted(3000);
	}

	private void handleStatus(String contentType, ChannelStatus channelStatus) {
		if(channelStatus == null)
			return;
		String status = channelStatus.getStatus();
		if(status != null) {
			switch (status) {
				case ChannelStatus.STATUS_CONNECTED:
					$(() -> {
//						imClient.sendData(new IncomingData().message(new TestItem().action("error"))).thenAccept(result -> {
//							assertNull(result);
//						}).exceptionally(throwable -> {
//							assertNotNull(throwable);
//							return null;
//						}).thenAccept(unused -> {
//							imClient.sendData(new IncomingData().message(new TestItem().action("normal"))).thenAccept(result -> {
//								assertNotNull(result);
//								assertNotNull(result.getCode());
//							}).exceptionally(throwable -> {
//								assertNull(throwable);
//								return null;
//							}).thenAccept(unused1 -> {
//								imClient.sendData(new IncomingData().message(new TestItem().action("normal1"))).thenAccept(result -> {
//									assertNotNull(result);
//									assertNotNull(result.getCode());
//									assertEquals("oops", ((TestItem)result.getMessage()).getAction());
//									completed();
//								}).exceptionally(throwable -> {
//									assertNull(throwable);
//									return null;
//								});
//							});

							imClient.sendData(new IncomingData().message(new TestItem().action("kick"))).whenComplete((result, throwable) -> {
								assertNotNull(throwable);
							});

//							long times = 1000;
//							long time = System.currentTimeMillis();
//							AtomicLong longAdder = new AtomicLong();
//							for(int i = 0; i < times; i++) {
//								imClient.sendData(new IncomingData().message(new TestItem().action("normal1"))).thenAccept(result -> {
//									assertNotNull(result);
//									assertNotNull(result.getCode());
//									assertEquals("oops", ((TestItem)result.getMessage()).getAction());
//
//									System.out.println("result " + result + " counter " + longAdder.incrementAndGet());
//									if(longAdder.longValue() == times) {
//										System.out.println("takes " + (System.currentTimeMillis() - time));
//										completed();
//									}
//								}).exceptionally(throwable -> {
//									assertNull(throwable);
//									return null;
//								});
//							}
//						});

					});
					break;
			}
		}
	}

	private void handleNewDataReceived(String contentType, OutgoingData outgoingData) {
		NewDataReceived newDataReceived = (NewDataReceived) outgoingData.getMessage();

	}
}
