package io.tapdata;

import io.tapdata.entity.SourceContext;
import io.tapdata.exception.SourceException;

/**
 * @author samuel
 * @Description
 * @create 2022-01-26 12:05
 **/
public class BaseSource extends BaseConnection implements Source {

	protected SourceContext sourceContext;

	@Override
	public void sourceInit(SourceContext context) throws SourceException {
		this.sourceContext = context;
	}

	@Override
	public void initialSync() throws SourceException {

	}

	@Override
	public void increamentalSync() throws SourceException {

	}

	@Override
	public void sourceStop(Boolean force) throws SourceException {

	}

	@Override
	public int getSourceCount() throws SourceException {
		return 0;
	}

	@Override
	public long getSourceLastChangeTimeStamp() throws SourceException {
		return 0;
	}
}
