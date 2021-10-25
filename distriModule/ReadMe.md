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



### 对齐阶段

基于集中式代码的逻辑，对现有程序进行优化。

![](/data/files/杂项/研究生阶段/后台程序运行截图/集中式目录结构.png)

将对齐处理好的一组文件的路径存储在 HBase 中，只存路径，即生成了索引。

然后将当前的处理结束位置记录在 KsLog 文件夹下，生成一个日志文件，防止下次执行程序时再次重头开始。这里需要细致设计一下。

日志文件信息结构参考 Nginx 服务器生成的日志。



**需求分析：**

目前有n个文件夹，每个文件夹代表一个波形监控器，各个文件夹中存储的数据即波形数据文件。根据 变量 k 作为阈值来终止回溯，这个变量是手动输入的，需要人工的控制。



**文件命名形式：**

![](/data/files/杂项/研究生阶段/后台程序运行截图/文件命名解析.png)

Test_没用，后面的是时间信息，比如 190923080744 就代表 19年9月23日8点零七分44秒事件的波形文件。



对齐的规则是寻找数组中 其他台站与其相差一个小时的文件。比如S监控器的 190923080744这个时间节点的事件文件，那么就要去找 T U V Y Z目录下的 190923 （07/08/09）44的文件。



要做的工作是要将所有台站中的文件根据文件名进行对齐操作，目前的想法 是将文件名全部进行切割组合并塞进一个Map结构，数据预处理后的形式大致是这样的：

![](/data/files/杂项/研究生阶段/后台程序运行截图/数据预处理封装成map形式.png)

**优化逻辑：**

如果k<3||k>n，则不足以计算，直接返回警告信息。



如果3<=k<=n，则对 Map 基于value进行 升序排序，这时的决策树如下图所示:（以k==n为例）

（紫色方块代表最终塞进res的子结果。红色代表此时穷举到的文件不合法，回溯终止，剪枝操作。其他颜色为每一层的合法选择）

![](/data/files/杂项/研究生阶段/后台程序运行截图/对齐阶段回溯决策树.png)



这其实就又转换成了一个经典的用回溯算法解决组合问题。



**算法设计：**

**isValid方法：**（将此时待选择时间与之前的track里面的所有元素比较，判断标准是一小时之内。如果true则继续向下，返回false的话返回backtrack方法后直接break，说明这一次回溯已经没有意义了，及时剪枝，降低复杂度）。

```java
    public static boolean isValid(LinkedList<String> track,String dateBeforeJudge) throws ParseException {
        for (String trackStr:track){
            //对数据进行切割，先看是否在同一天
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            Date trackStrDate = simpleDateFormat.parse(trackStr.substring(0,8));
            Date dateBeforeJudgeDate = simpleDateFormat.parse(dateBeforeJudge.substring(0,8));
            if (!trackStrDate.equals(dateBeforeJudgeDate)){
                System.out.println("不在同一天");
                return false;
            }
            //如果是同一天，则比较时间差是否在一个小时以内
            /*
            * 这里之后会封装成一个计算秒级别差值的方法
            * 这里这样写一定会加快速度，毕竟很多时间都执行不到这一步
            * 避免每次都要计算时间差
            * */
            SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss");
            Date trackStrDate1 = simpleDateFormat1.parse(trackStr);
            Date dateBeforeJudgeDate1 =  simpleDateFormat1.parse(dateBeforeJudge);
            //时间差
            int diff = (int)((trackStrDate1.getTime()-dateBeforeJudgeDate1.getTime())/1000);
            System.out.println("时间差:"+diff);
            if (Math.abs(diff)>3600){
                System.out.println("在同一天，但是时间差大于一个小时");
                return false;
            }
        }
        return true;
    }
```

**回溯 backtrack 方法：**

```java
 /*
    * 回溯递归方法
    * */
    public void backtrack(LinkedList<String> track,Map<Character,List<String>> map,List<Character> panfus,int start,int k) throws ParseException {
        if (track.size() == k){
            backTrackRes.add(new LinkedList<>(track));
            return;
        }
        List<String> tmpList = map.get(panfus.get(start));
        for (String date : tmpList) {
            /*
            * 用当前的日期的日期与track中其他日期对比，
            * 查看是否在同一个小时内
            * */
            if (!isValid(track, date))
                continue;
            track.add(date);
            backtrack(track, map, panfus, start + 1,k);
            track.removeLast();
        }
    }
```

此时的执行效率（4000个文件路径）：

k==n==6：用时 5.58s

![](/data/files/杂项/研究生阶段/后台程序运行截图/k等于6效率.png)

k==5<n：用时4.83s

k==4<n：用时4.47s

k==3<n：用时3.92s

**封装完整路径：**

![](/data/files/杂项/研究生阶段/后台程序运行截图/封装完整路径.png)

对齐部分完成！

### 将对齐结果存储在HBase中











