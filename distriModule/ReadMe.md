# KS后端分布式文档 # 

## 环境及相关参数配置 ##

基于 hadoop-3.1.3 、 hbase -2.3.6 、zookeeper-3.4.10、jdk1.8



实验测试环境是由三台 CentOS 虚拟机搭建而成的完全分布式集群，采用局域网进行连接。



**虚拟机的配置如下：**

两台213监控台主机各 分配了 5g内存、4核以及400G硬盘空间，分别命名为 hadoop100和hadoop101。

ip地址为：192.168.1.128 以及 192.168.1.161

一台个人笔记本虚拟主机，4核、5g以及200g硬盘空间，命名为hadoop102

ip地址为：192.168.1.162。



## 程序设计思路 ##

（1）数据预处理即对齐部分：

利用mapreduce对现有的大数据量历史数据进行处理，将已经对齐的文件按照文件夹的形式分类存储在 hdfs 中，

防治每次读程序都要重复之前的操作。

但是由于目前实验环境的空间仅有 1T ，故仅仅能存储 个别月份作为测试。



## 实现进度 ##

### 实验数据上传至 HDFS 

（1）在 HDFS 创建文件夹

```java
    /*
    * 创建文件夹方法
    * */
    public void mkDir(List<String> pathStr) throws IOException {
        for (String str:pathStr)
            fileSystem.mkdirs(new Path(str));
        System.out.println("创建文件夹成功");
    }
        @Test
        List<String> path = new ArrayList<>();
        path.add("/hy_history_data/September/S");path.add("/hy_history_data/September/U");
        path.add("/hy_history_data/September/T");path.add("/hy_history_data/September/V");
        path.add("/hy_history_data/September/Y");path.add("/hy_history_data/September/Z");
        upFileToHDFS.mkDir(path);
```

```java
    /*
    * 根据目录上传文件方法
    * */
    public void uploadFileByAbsolutePath() throws IOException {

        GetFileName getFileName = new GetFileName();
        String[]  diskName = {"Z"};
        for (String s : diskName) {
            List<String> fileAbsolutePathList = getFileName.filePathList("/run/media/han56/新加卷/红阳三矿/201909/" + s);
            int flag = 0;
            System.out.println("====正在上传"+s+"盘符的数据====");
            System.out.println("该盘符中共有"+fileAbsolutePathList.size()+"个.HMED后缀文件");
            for (String path : fileAbsolutePathList) {
                fileSystem.copyFromLocalFile(false, false,
                        new Path(path), new Path("hdfs://hadoop101/hy_history_data/September/" + s));
                flag++;
                System.out.println("第"+flag+"个文件:"+path + "  had uploaded to hdfs!");
            }
        }
    }
```

![](/data/files/杂项/公众号图片素材/Hadoop系列文章/分布式程序截图/创建文件夹.png)

（2）上传文件至各个文件夹

由于第一次集群所设置空间过小，不足以存储下实验数据。所以利用实验室现有的空闲20T硬盘组再次搭建了新的集群。

第二次搭建的集群注意事项：

一定要将集群各个节点的时间进行同步。最大打开文件数目以及用户最大进程数的调整。

hdfs:   dfs.max.transfer.threads 需要调整为 8192

由于空间不足的问题，考虑将磁盘组进行分区，并在各个分区上安装Hadoop集群分支，需要重置整个集群的设置。

利用闲置的硬盘组，进行分区，并在上面搭建 CentOS虚拟机，创建测试环境。

大文件传输 具体配置已在微信公众号中进行记录：

```url
https://mp.weixin.qq.com/s?__biz=Mzg3OTI1ODkzOQ==&mid=2247485618&idx=1&sn=0ea1df3836048c9b8afcafbb4fdc0125&chksm=cf0674e6f871fdf0a531fed4853e9eb559ffa4adc9c1ae89667d4aad198e4d0ddb9d23fcd615&token=678281616&lang=zh_CN#rd
```

可以连续上传超过 200G 的文件。



### 对齐阶段（第一次MapReduce）

示意图：

![](/data/files/杂项/研究生阶段/会议文档/第一次MapReduce架构.png)

对齐阶段十分符合MapReduce的处理逻辑。目前所有测试文件都存储在不同的集群节点中。

基于集中式代码的逻辑，将现有程序改为分布式处理。

![](/data/files/杂项/研究生阶段/后台程序运行截图/集中式目录结构.png)

将对齐处理好的一组文件的路径存储在 HBase 中，只存路径，即生成了索引。

然后将当前的处理结束位置记录在 KsLog 文件夹下，生成一个日志文件，防止下次执行程序时再次重头开始。这里需要细致设计一下。

具体是将所有记录写在一个文件中还是每个文件都是一次执行的记录需要研究。



**对齐MR程序部分：**

输入输出：



程序逻辑代码：



执行结果：



