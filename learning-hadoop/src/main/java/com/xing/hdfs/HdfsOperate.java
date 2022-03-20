package com.xing.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hdfs操作相关
 *
 * @author xingguo
 */
public class HdfsOperate {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //1获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");
        //2创建目录
        fs.mkdirs(new Path("/test/local/client"));
        //3关闭资源
        fs.close();
    }
}
