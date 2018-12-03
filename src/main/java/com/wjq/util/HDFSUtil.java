package com.wjq.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

/**
* @author: wangjq
* @date: 2018-04-17 14:31
*/
public class HDFSUtil {
	private static String keytabPath;
	private static String krbConfPath;
	private static Configuration conf;
	private static String hdfs_site;
	private static String core_site;
	private static String user;
	private static Logger log = LoggerFactory.getLogger(HDFSUtil.class);

	static{
		try{
			Properties properties = new Properties();
			InputStream is = HDFSUtil.class.getClassLoader().getResourceAsStream("config.properties");

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

	public HDFSUtil(){
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
		}catch (IOException e){
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}

	private void init() throws Exception{
		try{
			Properties properties = new Properties();
			InputStream is = HDFSUtil.class.getClassLoader().getResourceAsStream("config.properties");

			properties.load(is);
			user = properties.getProperty("ibdcUser");
			krbConfPath = properties.getProperty("krb5Conf");
			core_site = properties.getProperty("coreSite");
			hdfs_site = properties.getProperty("hdfsSite");
			keytabPath = properties.getProperty("keytabFile");
		}catch (Exception e){
			e.printStackTrace();
			throw new Exception("加载配置文件 config.properties 异常!", e);
		}
	}

	private void loginHdfs() throws Exception{
		if(conf == null){
			try {
				init();
				conf = new Configuration();
				conf.setBoolean("fs.hdfs.impl.disable.cache", true);
				System.setProperty("java.security.krb5.conf", krbConfPath);
				conf.set("hadoop.security.authentication", "kerberos");
				conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");
				conf.addResource(new Path(core_site));
				conf.addResource(new Path(hdfs_site));
				UserGroupInformation.loginUserFromKeytab(user, keytabPath);
			} catch (Exception e) {
				throw new Exception("加载配置文件 config.properties 异常!", e);
			}
			UserGroupInformation.setConfiguration(conf);
		}
	}

	private boolean hasPermission(AclStatus aclStatus, String user, String userGroup){
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

	public boolean testPathExists(String oozieUser, final String path) throws Exception{
		loginHdfs();
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser, UserGroupInformation.getLoginUser());
		return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
			public Boolean run()throws Exception {
				FileSystem fs = null;
				try{
					fs = FileSystem.get(conf);
					return fs.exists(new Path(path));
				}catch (Exception e){
					throw new Exception("hdfs路径检测异常!", e);
				}finally {
					IOUtils.closeStream(fs);
				}
			}
		});
	}

	public FileStatus getFileLinkStatus(String oozieUser, final String path) throws Exception{
		loginHdfs();
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser, UserGroupInformation.getLoginUser());
		return ugi.doAs(new PrivilegedExceptionAction<FileStatus>() {
			public FileStatus run()throws Exception {
				FileSystem fs = null;
				try{
					fs = FileSystem.get(conf);
					return fs.getFileLinkStatus(new Path(path));
				}catch (Exception e){
					throw new Exception("获取hdfs文件状态异常!", e);
				}finally {
					IOUtils.closeStream(fs);
				}
			}
		});
	}

	public boolean setAcl(String oozieUser, final String path, final List<AclEntry> aclSpec) throws Exception{
		loginHdfs();
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser, UserGroupInformation.getLoginUser());
		return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
			public Boolean run()throws Exception {
				FileSystem fs = null;
				try{
					fs = FileSystem.get(conf);
					fs.setAcl(new Path(path), aclSpec);
					return true;
				}catch (Exception e){
					throw new Exception("设置hdfs文件: " + path + " Acl异常!", e);
				}finally {
					IOUtils.closeStream(fs);
				}
			}
		});
	}

	public boolean deleteFile(String oozieUser, final String path) throws Exception{
		loginHdfs();
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser, UserGroupInformation.getLoginUser());
		return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
			public Boolean run() throws Exception {
				FileSystem fs = null;
				try{
					fs = FileSystem.get(conf);
					return fs.delete(new Path(path));
				}catch (Exception e){
					throw new Exception("删除hdfs文件异常!", e);
				}finally {
					IOUtils.closeStream(fs);
				}
			}
		});
	}

	public boolean sendFile(String oozieUser, final String path, final String localfile) throws Exception{
		loginHdfs();
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser,UserGroupInformation.getLoginUser());
		return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
			public Boolean run() throws Exception {
				return sendFile(path, localfile);
			}
		});
	}

	public boolean sendFile(String path, String localfile)throws Exception{
		File file = new File(localfile);
		if (!file.isFile() || !file.exists()) {
			throw new Exception("待上传本地文件" + localfile + " 不存在, 或者非文件! ");
		}
		FSDataOutputStream fsOut = null;
		FSDataInputStream fsIn = null;
		try {
			FileSystem localFS = FileSystem.getLocal(conf);
			FileSystem hadoopFS = FileSystem.get(conf);

			fsIn = localFS.open(new Path(localfile));
			fsOut = hadoopFS.create(new Path(path + "/" + file.getName()));
			byte[] buf = new byte[1024];
			int readbytes = 0;
			while ((readbytes = fsIn.read(buf)) > 0) {
				fsOut.write(buf, 0, readbytes);
			}
			return true;
		} catch (Exception e) {
			throw new Exception("上传hdfs文件异常!", e);
		} finally {
			IOUtils.closeStream(fsIn);
			IOUtils.closeStream(fsOut);
		}
	}

	public boolean mkdirs(String oozieUser, final String folder) throws Exception {
		loginHdfs();
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(oozieUser,
				UserGroupInformation.getLoginUser());
		return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
			public Boolean run() throws Exception {
				return mkdirs(folder);
			}
		});
	}

	public boolean mkdir(String user, final String path, final FsPermission fsPermission)throws Exception{
		boolean flag = false;
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
					throw new Exception("创建hdfs目录异常!", e);
				}
				return true;
			}
		});
		return flag;
	}

	public boolean mkdirs(final String folder)throws Exception {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path(folder);
			return fs.mkdirs(path);
		} catch (Exception e) {
			throw new Exception("创建hdfs目录异常!", e);
		} finally {
			IOUtils.closeStream(fs);
		}
	}

	public static List<AclEntry> get777Acl(){
		List<AclEntry> aclSpec = new ArrayList<>();
		aclSpec.add(AclEntry.parseAclEntry("user::rwx", true));
		aclSpec.add(AclEntry.parseAclEntry("group::rwx", true));
		aclSpec.add(AclEntry.parseAclEntry("other::rwx", true));
		return aclSpec;
	}
}
