package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class SqlToMqlUtil {

	private Document where;

	private Document filter;

	private Document sort;

	private int limit;

	private String collection;

	private String sql;

	private String err;

	private final static String SELECT = "select ";
	private final static String FROM = " from ";
	private final static String WHERE = " where ";
	private final static String ORDER_BY = " order by ";
	private final static String LIMIT = " limit ";
	private final static String AND = " and ";
	private final static String OR = " or ";

	public SqlToMqlUtil(String sql) {
		this.sql = sql.trim();
		where = new Document();
		filter = new Document();
		sort = new Document();
		limit = 0;
	}

	public boolean analyse() {
		if (checkSql()) {
			sql = StringUtils.removeStartIgnoreCase(sql, SELECT);
			if (StringUtils.endsWithIgnoreCase(sql, ";")) {
				sql = StringUtils.removeEndIgnoreCase(sql, ";");
			}
			handleFilter(sql);
			handleCollection(sql);
			sql = handleWhere(sql);
			if (sql == null) {
				return false;
			}
			handleLimit(sql);

			return true;
		} else {
			err = "sql syntax error, support format: select field1, field2 from table where field>1 and field='a' limit 10, ignore case";
			return false;
		}
	}

	private boolean checkSql() {
		return StringUtils.isNotBlank(sql)
				&& StringUtils.startsWithIgnoreCase(sql, SELECT)
				&& StringUtils.containsIgnoreCase(sql, FROM);
	}

	private void handleFilter(String sql) {
		if (StringUtils.isNotBlank(sql)) {
			int from = StringUtils.indexOfIgnoreCase(sql, FROM);
			if (from >= 0) {
				String field = sql.substring(0, from).trim();
				if (!field.equalsIgnoreCase("*")) {
					String[] fields = field.split(",");
					if (fields != null && fields.length > 0) {
						for (String s : fields) {
							filter.put(s.trim(), 1);
						}
					}
				}
			}
			this.sql = sql.substring(from + FROM.length()).trim();
		}
	}

	private void handleCollection(String sql) {
		if (StringUtils.isNotBlank(sql)) {
			int where = StringUtils.indexOfIgnoreCase(sql, WHERE);
			if (where >= 0) {
				collection = sql.substring(0, where).trim();
				this.sql = sql.substring(where + WHERE.length());
			} else {
				int limit = StringUtils.indexOfIgnoreCase(sql, LIMIT);
				if (limit >= 0) {
					collection = sql.substring(0, limit).trim();
					this.sql = sql.substring(limit);
				} else {
					collection = sql.trim();
					this.sql = "";
				}
			}
		}
	}

	private String handleWhere(String sql) {
		if (StringUtils.isNotBlank(sql)) {
			int limit = StringUtils.indexOfIgnoreCase(sql, LIMIT);
			String where;
			if (limit >= 0) {
				where = sql.substring(0, limit).trim();
				sql = sql.substring(limit + LIMIT.length());
			} else {
				where = sql.trim();
				sql = "";
			}
			if (StringUtils.isNotBlank(where)) {

				List<Document> documents = null;
				if (StringUtils.containsIgnoreCase(where, AND)) {
					documents = new ArrayList<>();
					this.where = new Document("$and", documents);
				}

				String[] wheres = where.split(AND);

				for (String s : wheres) {

					if (StringUtils.isNotBlank(s)) {

						String column = "";
						Object value = null;
						String compare = "";
						s = s.trim();
						char[] chars = s.toCharArray();

						for (int i = 0; i < chars.length; i++) {
							char c = chars[i];

							if (c != ' ' && c != '=' && c != '<' && c != '>') {
								column += c;
							} else {
								s = s.substring(i).trim();
								break;
							}
						}

						chars = s.toCharArray();
						if (chars[0] == '!' && chars[1] == '=') {
							compare = "!=";
							s = s.substring(2).trim();
						} else if (chars[0] == '>' || chars[0] == '<' || chars[0] == '=') {
							compare += chars[0];
							s = s.substring(1).trim();
							if (chars[1] == '=') {
								compare += chars[1];
								s = s.substring(1).trim();
							}
						} else {
							err = "cannot find condition(>, <, =, >=, <=, !=) in sql: " + sql;
							return null;
						}

						switch (compare) {
							case ">":
								compare = "$gt";
								break;

							case "<":
								compare = "$lt";
								break;

							case ">=":
								compare = "$gte";
								break;

							case "<=":
								compare = "$lte";
								break;

							case "=":
								compare = "$eq";
								break;

							case "!=":
								compare = "$ne";
								break;

							default:
								break;
						}

						if (StringUtils.isNotBlank(s)) {
							if (StringUtils.startsWithIgnoreCase(s, "\"")
									&& StringUtils.endsWithIgnoreCase(s, "\"")) {
								s = StringUtils.removeStartIgnoreCase(s, "\"");
								s = StringUtils.removeEndIgnoreCase(s, "\"");

								value = s;
							} else if (StringUtils.startsWithIgnoreCase(s, "\'")
									&& StringUtils.endsWithIgnoreCase(s, "\'")) {
								s = StringUtils.removeStartIgnoreCase(s, "\'");
								s = StringUtils.removeEndIgnoreCase(s, "\'");
								value = s;
							} else {
								try {
									value = Double.valueOf(s);
								} catch (NumberFormatException e) {
									err = "cannot get condition value, failed convert to double, value: " + s;
									return null;
								}
							}
						}

						if (StringUtils.isNotBlank(column) && StringUtils.isNotBlank(compare) && value != null) {
							if (documents != null) {
								documents.add(new Document(column, new Document(compare, value)));
							} else {
								this.where.put(column, new Document(compare, value));
							}
						}
					}
				}
			}
		}
		return sql.trim();
	}

	private void handleLimit(String sql) {
		if (StringUtils.isNotBlank(sql)) {
			try {
				this.limit = Integer.parseInt(sql);
			} catch (NumberFormatException e) {
				// do nothing
			}
		}
	}

	public Document getWhere() {
		return where;
	}

	public Document getFilter() {
		return filter;
	}

	public Document getSort() {
		return sort;
	}

	public int getLimit() {
		return limit;
	}

	public String getCollection() {
		return collection;
	}

	public String getErr() {
		return err;
	}

	@Override
	public String toString() {
		return "SqlToMqlUtil{" +
				"where=" + where.toJson() +
				", filter=" + filter.toJson() +
				", limit=" + limit +
				", collection='" + collection + '\'' +
				'}';
	}

	public static void main(String[] args) {
		String sql = "select * from test limit 10";
		SqlToMqlUtil sqlToMqlUtil = new SqlToMqlUtil(sql);
		sqlToMqlUtil.analyse();
		System.out.println(sqlToMqlUtil.toString());
	}
}
