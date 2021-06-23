package com.mit.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author mit
 * @Description TODO
 * @Date 2021/6/23 下午12:48
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接 hdfs nn 的uri
        URI uri = new URI("hdfs://hadoop-001:8020");
        // 新建配置文件
        Configuration config = new Configuration();
        config.set("dfs.replication", "2");
        // 用户
        String user = "root";

        // 1.获取客户端对象
        fs = FileSystem.get(uri, config, user);
    }

    @After
    public void close() throws IOException {
        // 3. 关闭客户端
        fs.close();
    }

    /**
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        // 2. 新建文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**
     * 文件上传
     * 参数优先级： hdfs-default.xml < hdfs-site.xml < 在项目资源目录下的配置文件 < 代码里面的配置
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        fs.copyFromLocalFile(false, true,
                new Path("/home/mit/tmp/hadoop/sunwukong.txt"),
                new Path("/xiyou/huaguoshan/"));

    }

    /**
     * 文件下载
     * @throws IOException
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false,
                new Path("/xiyou/huaguoshan"),
                new Path("/home/mit/tmp/hadoop"),
                false);
    }

    @Test
    public void testRm() throws IOException {
        // 参数二： 是否递归删除
        // 删除文件
        fs.delete(new Path("/jdk-8u212-linux-x64.tar.gz"), false);

        // 删除空目录
        fs.delete(new Path("/xiyou"), false);

        // 删除非空目录
        fs.delete(new Path("/jinguo"), true);
    }
}
