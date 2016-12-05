package com.ibeifeng.hadoop.hadoop;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSTest {
	// 读取hadoop里面的配置文件 core-default.xml, core-site.xml, hdfs-default.xml,
	// hdfs-site.xml,mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml
	// public static void main(String[] args) throws Exception{
	// Configuration configuration = new Configuration();
	// FileSystem fileSystem =FileSystem.get(new
	// URI("hdfs://Linux:8020"),configuration,"user");
	// System.out.println(configuration);
	// System.out.println(fileSystem);
	// }
	
	FileSystem fileSystem = null;
	/*
	 * 读取配置文件信息
	 */
	@Before
	public void getconf() throws Exception{
		Configuration conf = new Configuration();
		fileSystem = FileSystem.get(new URI("hdfs://Linux:8020"),conf,"user");
	}
	/*
	 * 从本地上传文件到hadoop
	 */
	//@Test
	public void upload() throws Exception{
		FileInputStream fileInputStream = new FileInputStream("d://123.jpg");
		FSDataOutputStream create = fileSystem.create(new Path("/output/test"),true);
		//true：如果发现已经有要上传的文件则覆盖
		IOUtils.copyBytes(fileInputStream, create, 4096,true);
	}
	/*
	 * 从hadoop中下载文件到本地
	 */
	//@Test
	public void download() throws Exception{
		FSDataInputStream open = fileSystem.open(new Path("/input/test.txt"));
		FileOutputStream fileOutputStream = new FileOutputStream("d://123.txt");
		IOUtils.copyBytes(open, fileOutputStream, 4096,true);
	}
	/*
	 * 创建文件夹
	 */
	//@Test
	public void create() throws Exception{
		boolean mkdirs = fileSystem.mkdirs(new Path("/lyj/lyj"));
		System.out.println(mkdirs);
	}
	/*
	 * 删除文件
	 */
	@Test
	public void deleate() throws Exception{
		boolean delete = fileSystem.delete(new Path("/output/256.jpg"),true);
		System.out.println(delete);
	}
}
