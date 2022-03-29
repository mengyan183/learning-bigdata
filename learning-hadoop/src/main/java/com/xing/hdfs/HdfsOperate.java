package com.xing.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * hdfs操作相关
 * WARN[org.apache.hadoop.util.NativeCodeLoader]-Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
 * 没有影响
 * hdfs的优点：
 * 1：高容错性（数据副本配置）
 * 2：适合大数据量处理
 * 3：机器配置要求简单
 * <p>
 * 缺点：
 * 1：不适合低延时数据访问
 * 2：无法高效的对大量小文件进行存储
 * 3：不支持并发写入以及文件随机修改，仅支持数据追加
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
            String s = uploadFile(fs, "/test/local/client");
            System.out.println(s);
        }
        //3关闭资源
        fs.close();
    }

    /**
     * 文件上传
     *
     * @param fileSystem 文件系统
     * @param destPath   hdfs 目标目录
     * @throws IOException
     */
    public static String uploadFile(FileSystem fileSystem, String destPath) throws IOException, URISyntaxException {
        fileSystem.deleteOnExit(new Path(new Path(destPath), new Path("uploadFile.txt")));
        URL resource = HdfsOperate.class.getClassLoader().getResource("uploadFile.txt");
        fileSystem.copyFromLocalFile(new Path(resource.toURI()), new Path(destPath));
        if (!fileSystem.exists(new Path(new Path(destPath), new Path("uploadFile.txt")))) {
            throw new RuntimeException("upload failed");
        }
        configurationPriority(fileSystem, new Path(new Path(destPath), new Path("uploadFile.txt")));
        return new Path(new Path(destPath), new Path("uploadFile.txt")).toString();
    }

    /**
     * 配置优先级，对于集群环境配置和本地客户端配置
     * 当本地客户端新增加了 hdfs-site.xml 并修改副本数量为1，重新上传文件，可以查看副本的数量变成了1
     * 因此 本地客户端配置会覆盖集群环境配置
     * 环境配置优先级为
     * 1：客户端代码中设置的值
     * 2：CLASSPATH中自定义配置文件（和远程环境文件名一致）
     * 3：远程服务器配置
     */
    public static void configurationPriority(FileSystem fileSystem, Path path) {
        short defaultReplication = fileSystem.getDefaultReplication(path);
        System.out.println("副本数量为" + defaultReplication);

    }
}
