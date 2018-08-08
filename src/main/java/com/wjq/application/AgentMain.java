package com.wjq.application;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import com.wjq.util.HdfsKbsProxyUtils;
import com.wjq.util.LogUtils;
import com.wjq.util.PropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.util.Date;
import java.util.List;

@Slf4j
public class AgentMain {

	public static void main(String[] args){
		initDFGXJob();
		initJDXTJob();
	}

	public static void initJDXTJob(){
		Runnable canPathRunnable = () -> {
			String srcPath = PropertiesUtil.getConfigValue("jdxtSrcPath");
			String errprPath = PropertiesUtil.getConfigValue("jdxtErrorPath");
			String completePath = PropertiesUtil.getConfigValue("jdxtCompletePath");
			String unknowPath = PropertiesUtil.getConfigValue("jdxtUnknow");

			String user = PropertiesUtil.getConfigValue("jdxtUser");
			String targetPath = "";
			String dateStr = "";
			while(true){
				try{
					List<String> listNames = FileUtil.listFileNames(srcPath);
					int i = 1;
					if(null != listNames && listNames.size()>0){
						LogUtils.info(user, "遍历文件总数: " + listNames.size());
						for(String fileName : listNames){
							LogUtils.info(user, "处理第" + i + "个文件 开始: " + fileName);
							if(!fileName.endsWith("csv")){
								FileUtil.move(new File(srcPath + fileName), new File(unknowPath + fileName), true);
								LogUtils.info(user, "非数据文件: " + fileName + ", 移到unknow path! ");
								continue;
							}
							int start = fileName.indexOf("20");
							dateStr = fileName.substring(start, start + 6);
							int date = Integer.parseInt(dateStr) + 1;

							targetPath = PropertiesUtil.getConfigValue("jdxtTargetPath") + String.valueOf(date);
							putFileToHdfs(srcPath, targetPath, fileName, completePath, user);

							LogUtils.info(user, "第" + i++ + "个文件, " + fileName + " 处理完毕. ");
							LogUtils.info(user, "----------------------------------");
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					LogUtils.error(user, "执行异常! " + e.getMessage());
				}
				try{
					//半小时执行一次
					Thread.currentThread().sleep(1000 * 60 * 30);
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		};
		new Thread(canPathRunnable).start();
	}

	private static void initDFGXJob() {
		Runnable canPathRunnable = () -> {
			String srcPath = PropertiesUtil.getConfigValue("dfgxSrcPath");
			String errprPath = PropertiesUtil.getConfigValue("dfgxErrorPath");
			String fileName1 = "mid_ida_tag_base_busn_mon";
			String fileName2 = "mid_ida_tag_base_mon";
			String fileName3 = "mid_ida_tag_user_amt_mon";
			String dateStr = DateUtil.format(new Date(), "yyyyMM");

			String completePath = PropertiesUtil.getConfigValue("dfgxCompletePath");
			String unknowPath = PropertiesUtil.getConfigValue("dfgxUnknow");
			String user = PropertiesUtil.getConfigValue("dfgxUser");
			String targetPath = "";
			while(true){
				try{
					List<String> listNames = FileUtil.listFileNames(srcPath);
					int i = 1;
					if(null != listNames && listNames.size()>0){
						LogUtils.info(user, "遍历文件总数: " + listNames.size());
						for(String fileName : listNames){
							LogUtils.info(user, "处理第" + i + "个文件 开始: " + fileName);
							if(fileName.startsWith("dir")){
								FileUtil.move(new File(srcPath + fileName), new File(unknowPath + fileName), true);
								LogUtils.info(user, "非数据文件: " + fileName + ", 移到error path! ");
								continue;
							}
							if(fileName.toLowerCase().indexOf(fileName1)>-1){
								//   /user/hive/warehouse/sztw.db/tb_mid_ida_tag_base_busn_mon/deal_date=201804
								dateStr = fileName.substring(25, 31);
								targetPath = PropertiesUtil.getConfigValue("dfgxTargetPath") + fileName1 + "/deal_date=" + dateStr;
								putFileToHdfs(srcPath, targetPath, fileName, completePath, user);
							}else if(fileName.toLowerCase().indexOf(fileName2)>-1){
								dateStr = fileName.substring(20, 26);
								targetPath = PropertiesUtil.getConfigValue("dfgxTargetPath") + fileName2 + "/deal_date=" + dateStr;
								putFileToHdfs(srcPath, targetPath, fileName, completePath, user);
							}else if(fileName.toLowerCase().indexOf(fileName3)>-1){
								dateStr = fileName.substring(24, 30);
								targetPath = PropertiesUtil.getConfigValue("dfgxTargetPath") + fileName3 + "/deal_date=" + dateStr;
								putFileToHdfs(srcPath, targetPath, fileName, completePath, user);
							}else{
								FileUtil.move(new File(srcPath + fileName), new File(errprPath + fileName), true);
							}
							LogUtils.info(user, "第" + i++ + "个文件, " + fileName + " 处理完毕. ");
							LogUtils.info(user, "----------------------------------");
						}
					}
				}catch (Exception e){
					e.printStackTrace();
					LogUtils.error(user, "执行异常! " + e.getMessage(), e);
				}
				try{
					// 2小时执行一次
					Thread.currentThread().sleep(1000 * 60 * 60 * 2);
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		};
		new Thread(canPathRunnable).start();
	}

	private static boolean putFileToHdfs(String srcPath, String targetPath, String fileName, String completePath, String user) {
		try{
			FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE);
			if(HdfsKbsProxyUtils.mkdir(user, targetPath, fsPermission)){
				LogUtils.info(user, "创建目录: " + targetPath + ", 成功! ");
				if(HdfsKbsProxyUtils.sendFile(user, targetPath, srcPath + fileName)){
					LogUtils.info(user, "发送文件: " + targetPath + ", " + srcPath + fileName + ", 成功! ");
					FileUtil.move(new File(srcPath + fileName), new File(completePath + fileName), true);
					LogUtils.info(user, "移动文件到 complete: " + fileName + ", 成功! ");
				}else{
					LogUtils.error(user, "发送文件: " + targetPath + "," + srcPath + fileName + ", 失败! ");
				}
			}else{
				LogUtils.error(user, "创建目录: " + targetPath + ", 失败! ");
			}
			return true;
		}catch (Exception e){
			e.printStackTrace();
			LogUtils.error(user, "发送文件 " + fileName + " 到 hdfs 异常! " + e.getCause().getMessage(), e);
			return false;
		}

	}

}
