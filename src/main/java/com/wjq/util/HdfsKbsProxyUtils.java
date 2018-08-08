package com.wjq.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

/**
* @author: wangjq
* @date: 2018-04-17 14:31
*/
public class HdfsKbsProxyUtils {
	private static String keytabPath;
	private static String krbConfPath;
	private static Configuration conf;
	private static String hdfs_site;
	private static String core_site;
	private static String user;
	private static Logger log = LoggerFactory.getLogger(HdfsKbsProxyUtils.class);

	static{
		try{
			Properties properties = new Properties();
			InputStream is = HdfsKbsProxyUtils.class.getClassLoader().getResourceAsStream("config.properties");
			properties.load(is);
			user = properties.getProperty("ibdcUser");
			krbConfPath = properties.getProperty("krb5Conf");
			core_site = properties.getProperty("coreSite");
			hdfs_site = properties.getProperty("hdfsSite");
			keytabPath = properties.getProperty("keytabFile");
		}catch (Exception e){
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}


	private static void loginHdfs()throws Exception {
		conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		System.setProperty("java.security.krb5.conf", krbConfPath);
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");
		conf.addResource(new Path(core_site));
		conf.addResource(new Path(hdfs_site));

		UserGroupInformation.setConfiguration(conf);
		try {
			UserGroupInformation.loginUserFromKeytab(user, keytabPath);
		} catch (IOException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			throw e;
		}
	}

	private static boolean hasPermission(AclStatus aclStatus, String user, String userGroup){
		if(aclStatus.getOwner().equalsIgnoreCase(user) || aclStatus.getGroup().equalsIgnoreCase(userGroup)){
			return true;
		}
		if(null!=aclStatus.getEntries() && aclStatus.getEntries().size()>0){
			for(AclEntry aclEntry : aclStatus.getEntries()){
				if(null != aclEntry.getName() && (aclEntry.getName().equalsIgnoreCase(user)
						|| aclEntry.getName().equalsIgnoreCase(userGroup))){
					return true;
				}
			}
		}
		return false;
	}

	public static void getContent(final String user, final String path){
		try{
			loginHdfs();
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() {
					FileSystem fileSystem;
					try {
						fileSystem = FileSystem.get(conf);
						if(fileSystem.exists(new Path(path))){
							FSDataInputStream inStream = fileSystem.open(new Path(path));
							BufferedReader bf=new BufferedReader(new InputStreamReader(inStream));//防止中文乱码
							String line = null;
							while ((line = bf.readLine()) != null) {
								System.out.println(line);
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					return null;
				}
			});
		}catch (Exception e){
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

	public static boolean sendFile(String oozieUser, final String path, final String localfile) throws Exception {
		boolean flag = false;
		try {
			loginHdfs();
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser,UserGroupInformation.getLoginUser());
			flag = ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
				public Boolean run()throws Exception {
					try{
						return doSendFile(path, localfile, oozieUser);
					}catch (Exception e){
						throw e;
					}
				}
			});
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return flag;
	}

	private static boolean doSendFile(String path, String localfile, String user) throws Exception{
		File file = new File(localfile);
		if (!file.isFile()) {
			throw new Exception(localfile + " is not a file!!");
		}
		FSDataOutputStream fsOut = null;
		FSDataInputStream fsIn = null;
		try {
			FileSystem localFS = FileSystem.getLocal(conf);
			FileSystem hadoopFS = FileSystem.get(conf);

			fsIn = localFS.open(new Path(localfile));

			if(hadoopFS.exists(new Path(path + "/" + file.getName()))){
				throw new Exception("hdfs 上文件已经存在! " + path + "/" + file.getName());
			}

			fsOut = hadoopFS.create(new Path(path + "/" + file.getName())/*, new Progressable(){
				@Override
				public void progress() {
					LogUtils.info(user, "文件: " + file.getName() + " 上传中...");
				}
			}*/);
			byte[] buf = new byte[64*1024];
			int readbytes = 0;
			while ((readbytes = fsIn.read(buf)) > 0) {
				fsOut.write(buf, 0, readbytes);
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (fsIn != null)
				try {
					fsIn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (fsOut != null)
				try {
					fsOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public static boolean mkdir(String user, final String path, final FsPermission fsPermission)throws Exception{
		boolean flag = false;
		try {
			loginHdfs();
			UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			flag = ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
				public Boolean run() throws Exception{
					FileSystem fileSystem;
					try {
						fileSystem = FileSystem.get(conf);
						if(!fileSystem.exists(new Path(path))){
							return fileSystem.mkdirs(new Path(path), fsPermission);
						}
					}catch (Exception e){
						e.printStackTrace();
						throw e;
					}
					return true;
				}
			});
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return flag;
	}
}
