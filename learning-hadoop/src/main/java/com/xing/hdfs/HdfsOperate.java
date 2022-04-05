package com.xing.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

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
            fs.delete(new Path("/test"), true);
            System.out.println("删除");
        } else {
            fs.mkdirs(new Path("/test/local/client"));
            System.out.println("新增");
            String s = uploadFile(fs, "/test/local/client");
            System.out.println(s);
            URL resource = HdfsOperate.class.getClassLoader().getResource("uploadFile.txt");
            String replace = resource.getPath().replace("uploadFile.txt", "copy_uploadFile.txt");
            downloadFile(fs, s, replace);
            rename(fs, s, s.replace("uploadFile.txt", "rename_uploadFile.txt"));
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


    /**
     * 将hdfs文件下载到本地
     * 对于useRawLocalFileSystem 默认为false，表示使用非本地模式，会在本地生成一个crc后缀的文件，表示校验文件的完整性
     * 修改为true 表示使用本地模式，则不会生成crc后缀文件
     *
     * @param fileSystem hdfs 文件系统
     * @param srcPath    hdfs中资源存储路径
     * @param desPath    本地资源存储路径
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void downloadFile(FileSystem fileSystem, String srcPath, String desPath) throws IOException, URISyntaxException {
//        fileSystem.copyToLocalFile(new Path(srcPath), new Path(desPath));
        // 该行代码等价于上一行代码
//        fileSystem.copyToLocalFile(false, new Path(srcPath), new Path(desPath), false);
        // 使用本地文件系统，不会生成crc后缀文件
        fileSystem.copyToLocalFile(false, new Path(srcPath), new Path(desPath), true);
        System.out.println(desPath + " is exists: " + Files.exists(Paths.get(desPath)));
    }

    /**
     * hdfs存储文件重命名
     *
     * @param fileSystem 文件系统
     * @param srcPath    hdfs存储的原文件名称
     * @param reNamePath hdfs存储的修改后文件名称
     * @throws IOException
     */
    public static void rename(FileSystem fileSystem, String srcPath, String reNamePath) throws IOException {
        fileSystem.rename(new Path(srcPath), new Path(reNamePath));
        System.out.println(reNamePath + " is exists : " + fileSystem.exists(new Path(reNamePath)));
    }
}
