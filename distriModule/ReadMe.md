# KS后端分布式文档 # 

## 环境及相关参数配置 ##

基于 hadoop-3.1.3 、 hbase -2.3.6 、zookeeper-3.4.10、jdk1.8、Flink 1.10.1、



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



### 第一次对齐阶段

基于集中式代码的逻辑，对现有程序进行优化。

![](/data/files/杂项/研究生阶段/后台程序运行截图/集中式目录结构.png)

将对齐处理好的一组文件的路径存储在 HBase 中，只存路径，即生成了索引。

然后将当前的处理结束位置记录在 KsLog 文件夹下，生成一个日志文件，防止下次执行程序时再次重头开始。这里需要细致设计一下。

日志文件信息结构参考 Nginx 服务器生成的日志。



**需求分析：**

目前有n个文件夹，每个文件夹代表一个波形监控器，各个文件夹中存储的数据即波形数据文件。根据 变量 k 作为阈值来终止回溯，这个变量是手动输入的，需要人工的控制。

{ 

​    {1,2,3,4,7,8},

​    {4,1,2,4,6,8},

​    {5,4,1,3,1},

​    {6,1,5,8,7,2},

​    {7,1,8,6,5,4} 

}

**文件命名形式：**

![](/data/files/杂项/研究生阶段/后台程序运行截图/文件命名解析.png)

Test_没用，后面的是时间信息，比如 190923080744 就代表 19年9月23日8点零七分44秒事件的波形文件。



对齐的规则是寻找数组中 其他台站与其相差一个小时的文件。比如S监控器的 190923080744这个时间节点的事件文件，那么就要去找 T U V Y Z目录下的 190923 （07/08/09）44的文件。



要做的工作是要将所有台站中的文件根据文件名进行对齐操作，目前的想法 是将文件名全部进行切割组合并，数据预处理后的形式大致是这样的：

![](/data/files/杂项/研究生阶段/后台程序运行截图/数据预处理封装成map形式.png)

**优化逻辑：**

如果k<3||k>n，则不可以计算，直接返回警告信息。



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

k==n==6：

![](/data/files/杂项/研究生阶段/后台程序运行截图/k等于6效率.png)

k==5<n：

![](/data/files/杂项/研究生阶段/后台程序运行截图/k=5的结果集.png)

k==4<n：

![](/data/files/杂项/研究生阶段/后台程序运行截图/k=4的结果集.png)

k==3<n：

![](/data/files/杂项/研究生阶段/后台程序运行截图/k=3的结果集.png)

**封装完整路径：**

对齐部分完成！

### 读取文件

将上文中初步对齐的结果集存储在 .txt 文件中，并且每一组以 \n 做分割![](/data/files/杂项/研究生阶段/后台程序运行截图/对齐结果写入文件.png)

测试阶段先放置在文件中，下一步将存储在HBase中做一个文件映射表，构成索引机制。



读取文件源代码分析：

![](/data/files/杂项/研究生阶段/后台程序运行截图/readData源代码.png)



unity 包中全部都是 bean 对象。对各种文件内容的数据模型定义。

在新的 分布式处理模块中，将 unity 包下的所有类定义在了 entity 包下，DataElement类也是个实体类。

其次，将util中的Byte2...类 改为 Byte2OtherFormatData类，并将原本的类简化成方法在一个类中保存。



最后，将ReadDateFromHead、ReadHfmedHead、ReadSensorProperties三个类中的方法放在了接口中，接口即方法的集合，在HDFSUtil类中实现接口并重载实现具体的逻辑代码。



其中，这里原来的问题不仅仅在模块管理混乱，而且譬如上述所说的三个类，功能严重重叠，几乎每个类都要进行一次IO操作重复读头文件数据。这样做的弊端不仅在多次IO上，且申请了众多缓冲区也会导致虚拟机认为该区域为热点区域，导致不会被释放。

所以，对此进行了优化，使其仅读取了一次，然后对数据进行封装。且使用了 hdfs 提供的API进行读取。它的好处在于比Java的内置类更加轻便，撇出了一些繁重的内容。

#### 读取文件接口

读取文件流程图：

![](/data/files/杂项/研究生阶段/后台程序运行截图/单文件读取测试流程图.png)

文件内部对齐过程：

对第一组测试数据特征进行分析

|  文件夹  |    S     |    Z     |    U     |    V     |    Y     |    T     |
| :------: | :------: | :------: | :------: | :------: | :------: | :------: |
| 开始时间 | 11:05:20 | 10:37:45 | 10:59:15 | 10:09:49 | 10:16:50 | 10:45:06 |
| 结束时间 | 12:05:20 | 11:37:45 | 11:59:15 | 11:09:49 | 11:16:50 | 11:45:06 |

内部对齐示意图：

![](/data/files/杂项/研究生阶段/后台程序运行截图/文件内部对齐示意图.png)

由示意图可知：

有效时间段的始末时间戳由源数据的两个值决定

（1）源数据最大时间戳  例：11:15:20——作为有效时间段的起始时间戳

（2） 源数据最小时间戳 例：10:09:49——+1h做有效数据的结束时间戳

这里的分析可以作为读取数据跳字节的参数来源。



待分析的源程序代码：这些代码主要看懂其逻辑，代码直接重写。

*this.setting()*

```java
	/**
	 * 通过通道数量设置跳过字节数以及通道判断标志位。
	 */
	private void settings(HfmedHead hfmedHead) {
		this.segmentNum = hfmedHead.getSegmentNum();// 从文件头中获得段的数量
		this.segmentRecNum = hfmedHead.getSegmentRecNum();// 获得每个段的数据条目数
		this.channelNum = hfmedHead.getChannelOnNum();

		if (channelNum == 7) {
			this.channel = 123456;
			this.datahead = 20;
			this.bytenum = 14;
			this.voltstart = 12;
			this.voltend = 13;
			this.manager.mix_flag1 = true;
		} else if (channelNum == 4) {
			this.channel = 456;
			this.datahead = 26;
			this.bytenum = 8;
			this.voltstart = 6;
			this.voltend = 7;
			this.manager.mix_flag2 = true;
		}

		// 混合状态下不判断通道溢出。
		if (manager.mix_flag1 && manager.mix_flag2) {
			Parameters.TongDaoDiagnose = 0;
		}
	}
```

*this.getDataElementFromDataBytes()*

```java
	/**
	 * 注意：dataBytes的字节数（下标），以及通道是哪几个，若123通道则必须放在x1，y1，z1中，456通道放在x2，y2，z2中。
	 * 马老师仪器由于更改了6通道，双量程，因此使用channel=123456条件进入。
	 */
	@SuppressWarnings("unused")
	private DataElement getDataElementFromDataBytes() {
		DataElement dataElement = new DataElement();

		if (channel == 456) {
			if (manager.isMrMa[sensorID] == true) {
				short x2 = readsan[0];
				short y2 = readsan[1];
				short z2 = readsan[2];
				dataElement.setX2(x2);
				dataElement.setY2(y2);
				dataElement.setZ2(z2);
			} else {
				short x2 = Byte2Short.byte2Short(dataByte[0], dataByte[1]);
				short y2 = Byte2Short.byte2Short(dataByte[2], dataByte[3]);
				short z2 = Byte2Short.byte2Short(dataByte[4], dataByte[5]);

				dataElement.setX2(x2);
				dataElement.setY2(y2);
				dataElement.setZ2(z2);
			}
		}
		if (channel == 123456) {
			if (manager.isMrMa[sensorID] == true) {
				short x1 = Byte2Short.byte2Short(readsan[0], readsan[1]);
				short y1 = Byte2Short.byte2Short(readsan[2], readsan[3]);
				short z1 = Byte2Short.byte2Short(readsan[4], readsan[5]);
				short x2 = Byte2Short.byte2Short(readsan[6], readsan[7]);
				short y2 = Byte2Short.byte2Short(readsan[8], readsan[9]);
				short z2 = Byte2Short.byte2Short(readsan[10], readsan[11]);
				dataElement.setX1(x1);
				;
				dataElement.setY1(y1);
				dataElement.setZ1(z1);
				dataElement.setX2(x2);
				dataElement.setY2(y2);
				dataElement.setZ2(z2);
			} else {
				short x1 = Byte2Short.byte2Short(dataByte[0], dataByte[1]);
				short y1 = Byte2Short.byte2Short(dataByte[2], dataByte[3]);
				short z1 = Byte2Short.byte2Short(dataByte[4], dataByte[5]);
				short x2 = Byte2Short.byte2Short(dataByte[6], dataByte[7]);
				short y2 = Byte2Short.byte2Short(dataByte[8], dataByte[9]);
				short z2 = Byte2Short.byte2Short(dataByte[10], dataByte[11]);

				dataElement.setX1(x1);
				dataElement.setY1(y1);
				dataElement.setZ1(z1);

				dataElement.setX2(x2);
				dataElement.setY2(y2);
				dataElement.setZ2(z2);
			}
		}
		return dataElement;
	}
```

以下是一些异常问题的善后操作，应该设立一个异常处理接口，将这些方法进行集中，而不是直接写在一个类中，导致实体类不像实体类，服务层不像服务层。



读取一秒数据：（申请变量）

```
	/** this is a vector used to store one second data. */
	private Vector<String> data;// Vector<String>(线程同步数据列表)
	/** when GPS signal has gone, its value become true */
	public boolean isBroken = false;
	/** 秒数计数器 , 每调用一次getData的时候 ，这个计数器就加一 ，表示加一秒 */
	public int timeCount = 0;
	/** the number of sensor. */
	private int sensorID = 0;
	/** the name of sensor. */
	private String sensorName = "";
	/** 调用次数 */
	private int countSetState = 0;

	/** 上次访问文件名 */
	private String nameF1 = " ";
	/** 数据段总数 */
	private int segmentNum;
	/** 每个数据段中数据的个数 */
	private int segmentRecNum;
	/** 通道个数 */
	private int channelNum;
	/** 通道个数字符串用于读取 */
	private int channel;
	/** 数据头、文件头、字节数、电压起始、电压结束 */
	private int datahead;
	/** 缓冲池大小，10个传感器*频率*10s时间。 */
	private int bufferPoolSize = 10 * (Parameters.FREQUENCY + 200) * 10;

	private int bytenum;
	private int voltstart;
	private int voltend;
	boolean flag1 = false;
	boolean flag2 = false;

	/** 第一条数据的日期 */
	private Date date = new Date();
	/** 通道单位大小 */
	private float chCahi;
	/** 最新文件所在的目录路径 */
	private String filePath;
	/** the file to read */
	private File file;
	/** 流的重定向 */
	private BufferedInputStream buffered;
	/** 存放文件的字节 */
	private byte[] dataByte;
	/** 对齐要跳过的字节 */
	private byte[] dataByte1;
	/** 存放1秒数据的字节 */
	private byte[] dataYiMiao;
	/** 三个字节进行显示 */
	private byte[] readsan;
	private String newS;

	private ADMINISTRATOR manager;
```



#### 异常处理接口

（2）（3）方法都是为了处理 segmentRecNum 与文件位数对应不准的问题。

（1）*this.formerDate()*：处理GPS压力跳秒加一问题

```java
	/**
	 * 刘老师仪器使用。 时间的规整函数，当GPS压力位出现跳秒时，我们进行时间+1秒操作。
	 */
	public String formerDate() {
		Calendar calendar = Calendar.getInstance(); // 内存溢出的出错位置
		calendar.setTime(this.date);
		calendar.add(Calendar.SECOND, timeCount);
		Date startDate1 = calendar.getTime();
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
		return format2.format(startDate1);
	}
```

（2）【自定义】前探方法：每次读取下一个数据段之前，需要用 ByteBuffer 装载下 下一个数据段的前四位查看是否为 HFMD 特征码，如果是这个码则说明已经数据段已经读完了，读到了下一秒的数据段头的特征码了，所以结束



（3）电压跳动处理方法：voltProcessing 用于查看高低电平是否变化，读数据段 14位/8位 中的末尾两位，如果高低电平结束，说明一秒的数据已经结束，即不一定要循环 segmentRecNum ，碰到这个之后就要跳出来读下一秒的数据了。



（4）文件末尾处理方法：即已经读到文件末尾了，准备后事。



### 计算过程 ###

#### 技术框架的选择

一.Flink

由于之前一直在进行分布式计算前的准备工作，所以注意点并未在这一方面。根据调研发现，MapReduce虽然 可以实现目前的工作，但是这种技术已经在被逐渐的替代，引自美团技术团队的实时数仓文章：

![](/data/files/杂项/研究生阶段/后台程序运行截图/flink优势.png)

Flink不仅能够在实时处理上进行应用，在处理离线的问题上同样十分出众。

首先要了解流处理 与 批处理的区别：

**批处理**的特点是有界、持久、大量，非常适合需要访问全套记录才能完成的计算工作，一般用于离线统计。

流处理的特点是无界、实时，无需针对整个数据集执行操作，而是对通过系统传输的每个数据项执行操作，一般用于实时统计。



在Spark的世界观中，一切都是由批次组成的，离线数据是一个大批次，而实时数据是由一个一个无限的小批次组成的。

而flink的世界观中，一切都是由流组成的，离线数据是有界限的流，实时数据是一个没有界限的流，这就是所谓的有界流和无界流。



**无界数据流：**有开始没结束，但是对于无界数据流来说我们无法判断何时所有数据已经到达，所以这种模式通常需要以特定 的顺序（例如事件发生的顺序），或者增加消息队列机制，以保证结果的完整性。



**有界数据流：**有界数据流有明确定义的开始和结束，可以在执行 任何计算之前通过获取所有数据来处理有界流，处理有界流不需要有序读取。目前我们的离线数据读取和计算工作就满足这一条件 。所以最终选择flink作为计算模块的核心框架。



FlinkML 基于Filnk平台的机器学习工具 使用Scala语言，但是FlinkML的机器学习论坛还很冷门，无法找到一些可以使用的资源。



（2）Spark ML lib

优势在于有丰富的ML论坛，可以提供现成的算法。



#### 参考论文

机器学习预测实验室地震



会议任务分配记录:



张翰林：找可以使CS CAD图背景变白的源代码。



CS 胡永亮  王凯路：预警界面配色 ，预警界面的文字对话框，CAD图的白色显示，预警算法（胡），时间序列预测的现有框架

左侧：短期预测                                                              右侧：中长期预测，时间次数 、能量、B值、矩张量 、能量频次比、震级 

预测数据库的列起始时间终止时间 三个点的平面坐标 能量 震级 ，  综合指数法左侧 or 右侧 待定。



王智涵 郑吉源：

3.0继续完善。



于海友：

对接程序确认。









