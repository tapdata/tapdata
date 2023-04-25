package io.tapdata.flow.engine.V2.sharecdc;

import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.io.Closeable;

/**
 * @author samuel
 * @Description
 * @create 2022-02-16 15:53
 **/
public interface ShareCdcReader extends Closeable {

	/**
	 * Init share cdc reader
	 *
	 * @param shareCdcContext
	 * @throws ShareCdcUnsupportedException If not available, will throw this type of exception
	 */
	void init(ShareCdcContext shareCdcContext) throws ShareCdcUnsupportedException;

	default void listen(StreamReadConsumer streamReadConsumer) throws Exception {
		throw new UnsupportedOperationException();
	}

	boolean isRunning();
}
