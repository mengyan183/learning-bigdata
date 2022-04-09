package com.xing.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * hdfs 原生io流操作
 */
public class HdfsIO {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //1获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行，设置hdfs 协议 配置自定义hadoop集群namenode地址
        // 集群namenode地址查看 cat $HADOOP_HOME/etc/hadoop/core-site.xml 中 fs.defaultFS 配置
        // 对于hadoop101域名需要在本地 /etc/hosts中配置 域名对应的ip
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "hadoop");
        String s = copyLocal(fs, "uploadFile.txt", "/localupload", "uploadFile.txt");
        System.out.println("从本地上传到远程的路径为" + s);
        String replace = Objects.requireNonNull(HdfsIO.class.getClassLoader().getResource("uploadFile.txt"))
                .toURI()
                .toString()
                .replace("uploadFile.txt", "remoteUploadFile.txt");
        java.nio.file.Path path = Paths.get(replace);
        // TODO : unix 系统下 无权限访问文件
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        copyRemote(fs, replace, s);
        fs.close();
    }

    /**
     * 将本地文件上传到远程
     * 1：将本地文件作为文件输入流
     * 2：将远程hdfs作为文件输出流
     * 3：流拷贝
     * 4：关闭流
     *
     * @param fileSystem     hdfs文件系统
     * @param localSrcPath   resources目录下的本地文件相对路径
     * @param remoteDestPath 远程文件路径
     * @param remoteFileName 远程文件名称
     * @return 远程文件路径地址
     */
    public static String copyLocal(FileSystem fileSystem, String localSrcPath, String remoteDestPath, String remoteFileName) throws IOException {
        // 获取输入流
        InputStream resourceAsStream = HdfsIO.class.getClassLoader().getResourceAsStream(localSrcPath);
        if (!fileSystem.exists(new Path(remoteDestPath))) {
            boolean mkdirs = fileSystem.mkdirs(new Path(remoteDestPath));
            if (!mkdirs) {
                throw new RuntimeException("远程文件夹[" + remoteDestPath + "]创建失败");
            }
        }
        // 获取输出流（通过创建一个新的路径，默认覆盖路径）
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(remoteDestPath, new Path(remoteFileName)));
        IOUtils.copy(resourceAsStream, fsDataOutputStream);
        IOUtils.close(fsDataOutputStream);
        IOUtils.close(resourceAsStream);
        Path remotePath = new Path(remoteDestPath, new Path(remoteFileName));
        if (fileSystem.exists(remotePath)) {
            return remotePath.toString();
        }
        throw new RuntimeException("local upload to remote failed");
    }

    /**
     * 将远程文件下载到本地
     * 1：判断远程文件是否存在
     * 2：将远程文件作为输入流
     * 3：建立本地文件作为输出流
     * 4：流数据拷贝
     * 5：关闭流
     *
     * @param fileSystem    hdfs文件系统
     * @param localDestPath 本地目标路径
     * @param remoteSrcPath 远程源路径
     */
    public static void copyRemote(FileSystem fileSystem, String localDestPath, String remoteSrcPath) throws IOException {
        boolean exists = fileSystem.exists(new Path(remoteSrcPath));
        if (!exists) {
            throw new RuntimeException("远程文件[" + remoteSrcPath + "]不存在");
        }
        try (FSDataInputStream open = fileSystem.open(new Path(remoteSrcPath))) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(localDestPath)) {
                IOUtils.copy(open, fileOutputStream);
            }
        }
    }
}
