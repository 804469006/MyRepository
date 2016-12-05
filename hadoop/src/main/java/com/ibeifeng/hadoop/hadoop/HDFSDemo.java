package com.ibeifeng.hadoop.hadoop;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
	FileSystem fileSystem = null;
	@Before
	public void getconf() throws IOException, InterruptedException,
			URISyntaxException {
		Configuration conf = new Configuration();
		fileSystem = FileSystem.get(new URI("hdfs://Linux:8020"), conf, "user");
	}
    /*
     * 从hadoop虚拟机中下载文件到本地
     */
	 //@Test
	public void testDownload() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fileSystem.open(new Path("/input/test.txt"));
		FileOutputStream out = new FileOutputStream("d://test.txt");
		IOUtils.copyBytes(in, out, 4096, true);
	}
    /*
     * 从本地上传文件到hadoop
     */
	 //@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		FSDataOutputStream out = fileSystem.create(new Path("/output/256.jpg"),
				true);
		FileInputStream in = new FileInputStream("D://123.jpg");
		IOUtils.copyBytes(in, out, 4096, true);
	}	 
	 /*
	  * 删除hadoop中的文件
	  */
	 //@Test
	public void testDel() throws IllegalArgumentException, IOException {
		boolean delete = fileSystem.delete(new Path("/output/256.jpg"), true);
		System.out.println(delete);
	}
	/*
	 * 创建文件夹
	 * 
	 */
	 //@Test
	public void testMkdir() throws IllegalArgumentException, IOException {
		boolean mkdirs = fileSystem.mkdirs(new Path("/zly/pic"));
		System.out.println(mkdirs);
	}

}
