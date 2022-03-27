package com.xing.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hdfs操作相关
 * WARN[org.apache.hadoop.util.NativeCodeLoader]-Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
 * 没有影响
 *
 * @author xingguo
 */
public class HdfsOperate {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //1获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行，设置hdfs 协议 配置自定义hadoop集群namenode地址
        // 集群namenode地址查看 cat $HADOOP_HOME/etc/hadoop/core-site.xml 中 fs.defaultFS 配置
        // 对于hadoop101域名需要在本地 /etc/hosts中配置 域名对应的ip
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");
        //2创建目录
        boolean exists = fs.exists(new Path("/test"));
        if (exists) {
            fs.deleteOnExit(new Path("/test"));
            System.out.println("删除");
        } else {
            fs.mkdirs(new Path("/test/local/client"));
            System.out.println("新增");
        }
        //3关闭资源
        fs.close();
    }
}
