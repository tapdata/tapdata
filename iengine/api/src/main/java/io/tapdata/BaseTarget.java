package io.tapdata;

import com.tapdata.entity.MessageEntity;
import io.tapdata.entity.OnData;
import io.tapdata.entity.TargetContext;
import io.tapdata.exception.TargetException;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-01-26 11:46
 **/
public class BaseTarget extends BaseConnection implements Target {

	protected TargetContext targetContext;

	@Override
	public void targetInit(TargetContext context) throws TargetException {
		this.targetContext = context;
	}

	@Override
	public OnData onData(List<MessageEntity> msgs) throws TargetException {
		return null;
	}

	@Override
	public void targetStop(Boolean force) throws TargetException {

	}

	@Override
	public int getTargetCount() throws TargetException {
		return 0;
	}

	@Override
	public long getTargetLastChangeTimeStamp() throws TargetException {
		return 0;
	}

	@Override
	public TargetContext getTargetContext() {
		return this.targetContext;
	}
}
