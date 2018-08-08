package com.wjq.util;

import cn.hutool.core.io.FileUtil;
import org.apache.log4j.*;

public class LogUtils {

	private static String basePah = PropertiesUtil.getConfigValue("logLocaltion");

	public static Logger getLoggerByName(String name){
		Logger logger = null;
		try{
			String realName = basePah + name.toLowerCase();
			logger = LogManager.getLoggerRepository().exists(realName);
			if(null == logger){
				logger = LogManager.getLogger(realName);
				logger.setAdditivity(true);
				PatternLayout layout = new PatternLayout("%d{yyy-MM-dd HH:mm:ss} --> %m%n");
				FileUtil.mkParentDirs(realName);
				DailyRollingFileAppender dailyRollingFileAppender = new DailyRollingFileAppender(layout, realName + ".log", ".yyyy-MM-dd");
				logger.addAppender(dailyRollingFileAppender);
				logger.setLevel(Level.INFO);
			};
		}catch (Exception e){
			e.printStackTrace();
		}
		return logger;
	}

	public static void info(String fileName, String msg, Exception e){
		Logger logger = getLoggerByName(fileName);
		logger.info(msg, e);
	}

	public static void info(String fileName, String msg){
		Logger logger = getLoggerByName(fileName);
		logger.info(msg);
	}

	public static void error(String fileName, String msg){
		Logger logger = getLoggerByName(fileName);
		logger.error(msg);
	}

	public static void error(String fileName, String msg, Exception e){
		Logger logger = getLoggerByName(fileName);
		logger.error(msg, e);
	}

	public static void warn(String fileName, String msg){
		Logger logger = getLoggerByName(fileName);
		logger.warn(msg);
	}
}
