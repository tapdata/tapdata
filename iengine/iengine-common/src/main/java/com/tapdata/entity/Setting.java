package com.tapdata.entity;

/**
 * Created by tapdata on 13/03/2018.
 */
public class Setting {

	private String _id;

	private String category;

	private String key;

	private String value;

	private String default_value;

	private String documentation;

	private long last_update;

	private String last_update_by;

	private String scope;

	private String category_sort;

	private String sort;

	private String key_label;

	private boolean user_visible;

	private boolean hot_reloading;

	public Setting() {
	}

	public Setting(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getDefault_value() {
		return default_value;
	}

	public void setDefault_value(String default_value) {
		this.default_value = default_value;
	}

	public String getDocumentation() {
		return documentation;
	}

	public void setDocumentation(String documentation) {
		this.documentation = documentation;
	}

	public long getLast_update() {
		return last_update;
	}

	public void setLast_update(long last_update) {
		this.last_update = last_update;
	}

	public String getLast_update_by() {
		return last_update_by;
	}

	public void setLast_update_by(String last_update_by) {
		this.last_update_by = last_update_by;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public String getCategory_sort() {
		return category_sort;
	}

	public void setCategory_sort(String category_sort) {
		this.category_sort = category_sort;
	}

	public String getSort() {
		return sort;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}

	public String getKey_label() {
		return key_label;
	}

	public void setKey_label(String key_label) {
		this.key_label = key_label;
	}

	public boolean isUser_visible() {
		return user_visible;
	}

	public void setUser_visible(boolean user_visible) {
		this.user_visible = user_visible;
	}

	public boolean isHot_reloading() {
		return hot_reloading;
	}

	public void setHot_reloading(boolean hot_reloading) {
		this.hot_reloading = hot_reloading;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("Setting{");
		sb.append("_id='").append(_id).append('\'');
		sb.append(", category='").append(category).append('\'');
		sb.append(", key='").append(key).append('\'');
		sb.append(", value='").append(value).append('\'');
		sb.append(", default_value='").append(default_value).append('\'');
		sb.append(", documentation='").append(documentation).append('\'');
		sb.append(", last_update='").append(last_update).append('\'');
		sb.append(", last_update_by='").append(last_update_by).append('\'');
		sb.append(", scope='").append(scope).append('\'');
		sb.append(", category_sort='").append(category_sort).append('\'');
		sb.append(", sort='").append(sort).append('\'');
		sb.append(", key_label='").append(key_label).append('\'');
		sb.append(", user_visible=").append(user_visible);
		sb.append(", hot_reloading=").append(hot_reloading);
		sb.append('}');
		return sb.toString();
	}
}
