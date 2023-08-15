package io.tapdata.http.util.engine;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @author lhm
 * @createTime 2021-01-07 15:03
 */
public class MysqlYear {

	private int year = 0;

	public void setYear(int year) {
		this.year = year;
	}

	public int getYear() {
		return year;
	}

	@Override
	public String toString() {
		return "" + year;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || getClass() != o.getClass()) return false;

		MysqlYear mysqlYear = (MysqlYear) o;

		return new EqualsBuilder()
				.append(getYear(), mysqlYear.getYear())
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(getYear())
				.toHashCode();
	}
}
