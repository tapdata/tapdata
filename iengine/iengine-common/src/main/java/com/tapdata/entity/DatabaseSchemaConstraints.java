package com.tapdata.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by tapdata on 14/12/2017.
 */
public class DatabaseSchemaConstraints {

	private String tableName;

	private String columnName;

	private String constraintName;

	private Integer position;

	private String fkTableName;

	private String fkColumnName;

	public DatabaseSchemaConstraints() {
	}

	/**
	 * pk constraint constructor
	 *
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	public static DatabaseSchemaConstraints pkConstraints(ResultSet resultSet) throws SQLException {
		DatabaseSchemaConstraints pkConstraints = new DatabaseSchemaConstraints();
		pkConstraints.setTableName(resultSet.getString(1));
		pkConstraints.setColumnName(resultSet.getString(2));
		pkConstraints.setConstraintName(resultSet.getString(3));
		pkConstraints.setPosition(resultSet.getInt(4));

		return pkConstraints;
	}

	/**
	 * fk constraint constructor
	 *
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	public static DatabaseSchemaConstraints fkConstraints(ResultSet resultSet) throws SQLException {
		DatabaseSchemaConstraints fkConstraints = new DatabaseSchemaConstraints();
		fkConstraints.setTableName(resultSet.getString(1));
		fkConstraints.setColumnName(resultSet.getString(2));
		fkConstraints.setConstraintName(resultSet.getString(3));
		fkConstraints.setPosition(resultSet.getInt(4));
		fkConstraints.setFkTableName(resultSet.getString(5));
		fkConstraints.setFkColumnName(resultSet.getString(6));

		return fkConstraints;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getConstraintName() {
		return constraintName;
	}

	public void setConstraintName(String constraintName) {
		this.constraintName = constraintName;
	}

	public Integer getPosition() {
		return position;
	}

	public void setPosition(Integer position) {
		this.position = position;
	}

	public String getFkTableName() {
		return fkTableName;
	}

	public void setFkTableName(String fkTableName) {
		this.fkTableName = fkTableName;
	}

	public String getFkColumnName() {
		return fkColumnName;
	}

	public void setFkColumnName(String fkColumnName) {
		this.fkColumnName = fkColumnName;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("OracleSchemaConstraints{");
		sb.append("tableName='").append(tableName).append('\'');
		sb.append(", columnName='").append(columnName).append('\'');
		sb.append(", constraintName='").append(constraintName).append('\'');
		sb.append(", position=").append(position);
		sb.append(", fkTableName='").append(fkTableName).append('\'');
		sb.append(", fkColumnName='").append(fkColumnName).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
