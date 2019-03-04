import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    // 上传文件
    public static void main(String[] args) throws IOException, InterruptedException,
            URISyntaxException {
// 1 获取文件系统
        Configuration configuration = new Configuration();
// 配置在集群上运行
// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
// FileSystem fs = FileSystem.get(configuration);
//        FileSystem fs = FileSystem.get(FileSystem.getDefaultUri("")), configuration,  "atguigu");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "hcq");
// 2 上传文件
        fs.copyFromLocalFile(new Path("d:/hello.txt"), new Path("/hello666.txt"));
// 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    public FileSystem init() {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
// 配置在集群上运行
// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
// FileSystem fs = FileSystem.get(configuration);
//        FileSystem fs = FileSystem.get(FileSystem.getDefaultUri("")), configuration,  "atguigu");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "hcq");
        } catch (IOException e) {
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fs;
    }

    @Test
    public void testDownload() throws IOException {
        FileSystem fs = init();
        // 2 执行下载操作
// boolean delSrc 指是否将原文件删除
// Path src 指要下载的文件路径
// Path dst 指将文件下载到的路径
// boolean useRawLocalFileSystem 是否开启文件效验
//        fs.copyToLocalFile(false, new Path("/hello1.txt"), new Path("e:/hello1.txt"), true);
        fs.copyToLocalFile(new Path("/hello2.txt"), new Path("d:/hello2.txt"));
// 3 关闭资源
        fs.close();
    }
    //* 用户上传

    //* 目录创建
    @Test
    public void testMkDir() throws IOException {
        FileSystem fs = init();
//        fs.copyToLocalFile(false, new Path("/hello1.txt"), new Path("e:/hello1.txt"), true);
        fs.mkdirs(new Path("/testDir1/dir"));
// 3 关闭资源
        fs.close();
    }

    //* 文件夹删除
    @Test
    public void testDeleteDir() throws IOException {
        FileSystem fs = init();
//        删除目录如果里面有文件，需要递归删除,recursive(递归)传true
//        fs.delete(new Path("/testDir1"),true);

//        如果不用递归则会报错
//        org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.fs.PathIsNotEmptyDirectoryException): `/testDir1 is non empty': Directory is not empty
        fs.delete(new Path("/testDir1"), false);

// 3 关闭资源
        fs.close();
    }

    //* 文件名更改
    @Test
    public void testRenameFile() throws IOException {
        FileSystem fs = init();
        fs.rename(new Path("/testDir1/dir"), new Path("/testDir1/dir2"));

// 3 关闭资源
        fs.close();
    }

    //* HDFS文件详情查看
    @Test
    public void testLookFileDetail() throws IOException {
        FileSystem fs = init();
        //遍历hcq用户根目录下的文件
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus status = locatedFileStatusRemoteIterator.next();
// 输出详情
// 文件名称
            System.out.println(status.getPath().getName());
// 长度
            System.out.println(status.getLen());
// 权限
            System.out.println(status.getPermission());
// 组
            System.out.println(status.getGroup());
// 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
// 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------------------");
        }
// 3 关闭资源
        fs.close();
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException,
            URISyntaxException {
// 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,
                "hcq");
// 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("d:/hello.txt"));
// 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hello4.txt"));
// 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);
// 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException,
            URISyntaxException{
// 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,
                "hcq");
// 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hello4.txt"));
// 3 获取输出流
        IOUtils.copyBytes(fis, System.out, configuration);
// 4 流对拷
// 5 关闭资源
        IOUtils.closeStream(fis);
    }

}
