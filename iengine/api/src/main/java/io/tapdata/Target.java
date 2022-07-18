package io.tapdata;

import com.tapdata.entity.MessageEntity;
import io.tapdata.entity.OnData;
import io.tapdata.entity.TargetContext;
import io.tapdata.exception.TargetException;

import java.util.List;

public interface Target extends TapInterface {

	/**
	 * @param context
	 * @throws TargetException
	 */
	void targetInit(TargetContext context) throws TargetException;

	/**
	 * @param msgs
	 * @throws TargetException
	 */
	OnData onData(List<MessageEntity> msgs) throws TargetException;

	/**
	 * @param force
	 * @throws TargetException
	 */
	void targetStop(Boolean force) throws TargetException;

	/**
	 * @return
	 * @throws TargetException
	 */
	int getTargetCount() throws TargetException;

	/**
	 * @return
	 * @throws TargetException
	 */
	long getTargetLastChangeTimeStamp() throws TargetException;

	TargetContext getTargetContext();

	default <E> void createTable(E createTable, String tableName) throws TargetException {
		throw new UnsupportedOperationException();
	}
}
