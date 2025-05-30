package com.tapdata.http.useragent;

import cn.hutool.core.collection.CollUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 平台对象
 *
 * @author looly
 * @since 4.2.1
 */
public class Platform extends UserAgentInfo {
	private static final long serialVersionUID = 1L;

	/**
	 * 未知
	 */
	public static final Platform Unknown = new Platform(NameUnknown, null);

	/**
	 * Iphone
	 */
	public static final Platform IPHONE = new Platform("iPhone", "iphone");
	/**
	 * ipod
	 */
	public static final Platform IPOD = new Platform("iPod", "ipod");
	/**
	 * ipad
	 */
	public static final Platform IPAD = new Platform("iPad", "ipad");

	/**
	 * android
	 */
	public static final Platform ANDROID = new Platform("Android", "android");

	/**
	 * harmony
	 */
	public static final Platform HARMONY = new Platform("Harmony", "OpenHarmony");
	/**
	 * android
	 */
	public static final Platform GOOGLE_TV = new Platform("GoogleTV", "googletv");

	/**
	 * Windows Phone
	 */
	public static final Platform WINDOWS_PHONE = new Platform("Windows Phone", "windows (ce|phone|mobile)( os)?");

	/**
	 * 支持的移动平台类型
	 */
	public static final List<Platform> mobilePlatforms = CollUtil.newArrayList(//
			WINDOWS_PHONE, //
			IPAD, //
			IPOD, //
			IPHONE, //
			new Platform("Android", "XiaoMi|MI\\s+"), //
			ANDROID, //
			GOOGLE_TV, //
			new Platform("htcFlyer", "htc_flyer"), //
			new Platform("Symbian", "symbian(os)?"), //
			new Platform("Blackberry", "blackberry"), //
			HARMONY
	);

	/**
	 * 支持的桌面平台类型
	 */
	public static final List<Platform> desktopPlatforms = CollUtil.newArrayList(//
			new Platform("Windows", "windows"), //
			new Platform("Mac", "(macintosh|darwin)"), //
			new Platform("Linux", "linux"), //
			new Platform("Wii", "wii"), //
			new Platform("Playstation", "playstation"), //
			new Platform("Java", "java") //
	);

	/**
	 * 支持的平台类型
	 */
	public static final List<Platform> platforms;

	static {
		platforms = new ArrayList<>(13);
		platforms.addAll(mobilePlatforms);
		platforms.addAll(desktopPlatforms);
	}

	/**
	 * 构造
	 *
	 * @param name  平台名称
	 * @param regex 关键字或表达式
	 */
	public Platform(String name, String regex) {
		super(name, regex);
	}

	/**
	 * 是否为移动平台
	 *
	 * @return 是否为移动平台
	 */
	public boolean isMobile() {
		return mobilePlatforms.contains(this);
	}

	/**
	 * 是否为Iphone或者iPod设备
	 *
	 * @return 是否为Iphone或者iPod设备
	 * @since 5.2.3
	 */
	public boolean isIPhoneOrIPod() {
		return this.equals(IPHONE) || this.equals(IPOD);
	}

	/**
	 * 是否为Iphone或者iPod设备
	 *
	 * @return 是否为Iphone或者iPod设备
	 * @since 5.2.3
	 */
	public boolean isIPad() {
		return this.equals(IPAD);
	}

	/**
	 * 是否为IOS平台，包括IPhone、IPod、IPad
	 *
	 * @return 是否为IOS平台，包括IPhone、IPod、IPad
	 * @since 5.2.3
	 */
	public boolean isIos() {
		return isIPhoneOrIPod() || isIPad();
	}

	/**
	 * 是否为Android平台，包括Android和Google TV
	 *
	 * @return 是否为Android平台，包括Android和Google TV
	 * @since 5.2.3
	 */
	public boolean isAndroid() {
		return this.equals(ANDROID) || this.equals(GOOGLE_TV);
	}

	/**
	 * 是否为Harmony平台
	 *
	 * @return 是否为Harmony平台
	 */
	public boolean isHarmony() {
		return this.equals(HARMONY);
	}

}
