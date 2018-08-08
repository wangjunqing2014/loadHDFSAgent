package com.wjq.util;

import cn.hutool.setting.dialect.Props;

public class PropertiesUtil {
	public static Props configProperties = new Props("config.properties");

	public static String getConfigValue(String key){
		return configProperties.getStr(key);
	}
}
