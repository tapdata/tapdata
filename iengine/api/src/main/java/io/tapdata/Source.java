package io.tapdata;

import io.tapdata.entity.SourceContext;
import io.tapdata.exception.SourceException;

public interface Source extends TapInterface {

	/**
	 * @param context
	 * @throws SourceException
	 */
	void sourceInit(SourceContext context) throws SourceException;

	/**
	 * @throws SourceException
	 */
	void initialSync() throws SourceException;

	/**
	 * @throws SourceException
	 */
	void increamentalSync() throws SourceException;

	/**
	 * @param force
	 * @throws SourceException
	 */
	void sourceStop(Boolean force) throws SourceException;

	/**
	 * @return
	 * @throws SourceException
	 */
	int getSourceCount() throws SourceException;

	/**
	 * @return
	 * @throws SourceException
	 */
	long getSourceLastChangeTimeStamp() throws SourceException;


}
