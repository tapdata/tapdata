package io.tapdata.common.ddl.wrapper;

/**
 * @author samuel
 * @Description
 * @create 2022-06-29 20:58
 **/
public abstract class BaseDDLWrapper<T> implements DDLWrapper<T> {
	protected DDLWrapperConfig ddlWrapperConfig;

	@Override
	public void init(DDLWrapperConfig ddlWrapperConfig) {
		this.ddlWrapperConfig = ddlWrapperConfig;
	}
}
