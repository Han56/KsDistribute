# 	                            KS后端分布式文档 # 

## 环境及相关参数配置 ##

各组件版本号： **hadoop-3.1.3 、 hbase -2.3.6 、zookeeper-3.4.10、jdk1.8**



实验测试环境是由**三台 CentOS 腾讯轻量云服务器搭建的完全分布式集群**。

![](/data/files/杂项/研究生阶段/后台程序运行截图/搭建集群/三台云服务器真集群.png)

|      | Hadoop101(Master)      | Hadoop102(slave1)             | Hadoop103(slave2)            |
| ---- | ---------------------- | ----------------------------- | ---------------------------- |
| HDFS | *NameNode* DataNode    | DataNode                      | *SecondaryNameNode* DataNode |
| YRAN | NodeManager jobhistory | *ResourceManager* NodeManager | NodeManager                  |



## 整体思路 ##

**目的：**筛选出历史波形文件中微震事件的前兆信息。

**问题：**微震事件预测预警模型 

（1）基于历史数据构建分布式的决策树模型

​          1.1 存储原始波形数据 以及 清洗过程  **着重看存储方面的创新点**（总结于HBase存储速度优化部分）

​          1.2  指标的计算

​          1.3  构建决策树    **决策树根节点选择的优化问题**

（2）模型的实时更新：**机器学习（决策树）模型更新时刻的优化研究**

​    

与Grapth进行结合的研究查阅。



我的想法是  在大量的历史数据中，我们已经知道具体哪天发生了微震事件

假设 2月26日发生了事件，1月26日也发生了一次事件，那么这一个月之间的数据就是要重点关注的范围，



- 根据师兄的建议，可以将微震波判识计算公式里面的长短时域进行调整，比如减小长时窗来筛选出更多更小的事件。

- 或者限制时间区域条件 ： 比如1月26日后一星期的所有数据+2月26日之前一星期的所有数据。




拿到这些数据后干什么？

可以通过相关计算公式计算出每个时间戳对应的微震属性（即前兆信息指标：B值 能量  危险事件数 震级连续大于1级次数  满足其中两个条件就会报警）

寻找这段时间中 前兆信息指标数据 特别异常的区域并在数据库中做上标记。即作为本次微震事件的前兆信息。

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/前兆信息HBase标记.png)



其次，这是后续的计划，不在当前主要问题。

假设我们现在已经拿到通过所有历史数据推断出来的前兆信息，可以将这些前兆信息的指标数值作为对未来事件预警的阈值，通过SparkML中的决策树+消息队列机制 向目前的系统中加入**实时预警推送机制**。（潜在的隐患在于原始文件过大，内存可能撑不住）

具体的流程图如下图所示：

 ![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/预警流程图.png)

决策树逻辑简要模型如下所示：假设满足两个前兆数值条件就会报警

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/微震预警决策树模型.png)



## 实现进度 ##

### 系统功能模块图

![](/data/files/杂项/研究生阶段/论文/大数据论文/汇报截图/系统功能模块图.png)

### 系统架构图

![](/data/files/杂项/研究生阶段/论文/大数据论文/汇报截图/系统架构图.png)




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



### 外部对齐阶段

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

以上都是对单个文件的读取操作，且源程序通过多线程来做到并行读取的。那么切换到分布式集群运行的话就不需要对程序本身做并行处理了，因为集群的存在就是为了解决并行程序的，只需要将对单个文件操作的程序放到集群上跑就可以了。



#### 异常处理接口

（2）（3）方法都是为了处理 segmentRecNum 与文件位数对应不准的问题。

（1）*this.formerDate()*：处理GPS压力跳秒加一问题

```java
    /*
    * 解决每个频率周期数据结束
    * 根据timeCount即可得到下一秒的数据
    * */
    @Override
    public String formerDate(String segHeadDate,int timeCount) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 24小时制
        //引号里面个格式也可以是 HH:mm:ss或者HH:mm等等，很随意的，不过在主函数调用时，要和输入的变
        //量day格式一致
        Date date = null;
        try {
            date = format.parse(segHeadDate);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (date == null)
            return "";
        System.out.println("front:" + format.format(date)); //显示输入的日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.SECOND, timeCount);// 24小时制
        date = cal.getTime();
        System.out.println("after:" + format.format(date));  //显示更新后的日期
        return format.format(date);
    }
```



（2）电压跳动处理方法：voltProcessing 用于查看高低电平是否变化，读数据段 14位/8位 中的末尾两位，如果高低电平结束，说明一秒的数据已经结束，即不一定要循环 segmentRecNum ，碰到这个之后就要跳出来读下一秒的数据了。

```java
 /*
    * 高低电平——一秒数据段结束标志
    * */
    @Override
    public boolean voltProcessing(short voltValue, int loopCount) {
        if (!isBroken){
            if (loopCount>(Parameters.FREQUENCY+210)){
                isBroken=true;
                System.out.println("该文件出现GPS缺失");
                timeCount++;
                flag1=flag2=false;
                return true;
            }
            //判断1s是否结束，结束跳出while
            if (Math.abs(voltValue)<1000)
                flag2=true;
            if (Math.abs(voltValue)>5000&&flag2)
                flag1=true;
            //高电平结束，说明1s数据结束
            if (flag1&&flag2){
                timeCount++;
                flag1=flag2=false;
                return true;
            }
        }else {
            //在对齐时就出现电压缺失，直接到这部分
            if (loopCount>=(Parameters.FREQUENCY+200)){
                timeCount++;
                flag1=flag2=false;
                return true;
            }
        }
        return false;
    }
```



（3）文件末尾处理方法：即已经读到文件末尾了，准备换下一个文件或者结束循环。



根据学长的源码，可以分为一下情形：

- **第一种情形**：读到的数据长度 < 通道数*2 说明这个文件的末尾无法提供足够的长度了，也是结束的标志。

- **第二种情形**：读到数据长度 == -1 说明当前这个文件已经被榨干了，换下一个接着榨。

- 第三种情形：读到数据长度 <  通道数*2 但是后面还有数据，要跳过这条数据，这个没关系可以不用处理，可以在后期进行清理，清除掉某个属性值为空的数据即可完成。

  

### 存储过程——离线数据仓库 ###

数据表结构：

![](/data/files/杂项/研究生阶段/后台程序运行截图/存储模块/HBase预期格式.png)



disquake分布式HBase数据表最终结构：

![](/data/files/杂项/研究生阶段/后台程序运行截图/存储模块/disquake表结构.png)

参考文章：

```
https://mp.weixin.qq.com/s__biz=Mzg3OTI1ODkzOQ==&mid=2247485840&idx=1&sn=48eb18b41c62c767c88722b36bf5eece&chksm=cf0675c4f871fcd27ad42e0e27e74359e7b3cd9e9279f4c08d90702811dd3070caa45d033706&token=1445788343&lang=zh_CN#rd
```

利用Versions的特性，使同一个RowKey下可以存储5000条数据，即一秒钟的数据，也可能是 5100 条数据每秒，将其设置为5150防治溢出。

时间为RowKey。

![](/data/files/杂项/研究生阶段/后台程序运行截图/存储模块/虚拟机集群配置.png)

**由于集群配置极低**，导致数据存储速度过慢，且已经将客户端的优化做了最大的努力，为了不影响后续的进度这一步只能暂时跳过。影响HBase写入性能的问题从宏观上来讲主要有三个方面：

（1）服务端：主要是集群的物理配置。以当前的配置仅能一秒插入100条数据，这与大数据框架根本没啥关系了。16G内存左右的标准集群在客户端为多线程插入的条件下可以做到 1秒钟 数十万条。

（2）客户端：插入的方式。

（3）网络I/O以及磁盘I/O，这并不是最主要的。

**客户端优化主要有三方面：**

（1）【参考文献：4】将客户端的阻塞式写入改为非阻塞式写入，但是这样做的问题在于客户端能够将巨量的数据发给服务端，但是服务端无法写入。

（2）客户端改为多线程批量插入，与（1）的问题一样，集群无法处理这么多数据。

（3）MapReduce并行Put，但这需要大量存储空间存储解析之后的十进制数据做中间介质。



### 清洗过程 ###

（1）目前存储的数据还有极小部分的数据冒头问题，具体问题如下图所示。

![](/data/files/杂项/研究生阶段/后台程序运行截图/数据冒头演示图.png)

不过这样的问题极其微小，但是为了不影响计算的精度，还是应该将数据进行清洗。不清洗的话就要考虑加在后面对计算结果的影响。

（2）坐标的差距不能太大  比如 出现 0 之类的就不能要了



### 计算过程 ###

#### 计算模块的公式

##### 前兆信息指标

通过论文中总结的微震前兆的大致信息如下图所示：

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/微震预测权值分析层次结构.png)



##### 微震波判识计算公式

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/微震波判识计算公式.png)

调整长时窗，缩短至 300ms ，保持短时窗 100ms 不变，这样符合分析条件的事件会更多。

由于分析的数据是历史数据，所以根据事件表中的结果，反推前兆信息的时间。然后截取前兆信息波形：

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/测试数据选择20年一月份.png)

可以看出红阳三矿2020一月份的事件偏多，所以 以一月做实验数据。



##### 微震到时定位计算公式

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/微震到时定位计算模块.png)

##### 微震震级计算公式

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/微震震级计算公式.png)

微震活动性的判断并不能通过单一的某个微震事件来决定，井下复杂的环境下，单一的微震事件并不具有代表性，因此，微震参数统计规律也是建立在大量微震事件基础之上。在地震学上，常用的统计学参数有微震能量指数、视体积、施密特数、微震能量释放率、B值、视应力等。



##### b值计算方法

使用适用于分析小震活动性的最大似然法进行估算b值



#### 技术框架的选择

一.Flink

由于之前一直在进行分布式计算前的准备工作，所以注意点并未在这一方面。根据调研发现，MapReduce虽然 可以实现目前的工作，但是这种技术已经在被逐渐的替代，引自美团技术团队的实时数仓文章：

![](/data/files/杂项/研究生阶段/后台程序运行截图/flink优势.png)

Flink不仅能够在实时处理上进行应用，在处理离线的问题上同样十分出众。

首先要了解 流处理 与 批处理的区别：

**批处理**的特点是有界、持久、大量，非常适合需要访问全套记录才能完成的计算工作，一般用于离线统计。

流处理的特点是无界、实时，无需针对整个数据集执行操作，而是对通过系统传输的每个数据项执行操作，一般用于实时统计。



在Spark的世界观中，一切都是由批次组成的，离线数据是一个大批次，而实时数据是由一个一个无限的小批次组成的。

而flink的世界观中，一切都是由流组成的，离线数据是有界限的流，实时数据是一个没有界限的流，这就是所谓的有界流和无界流。



**无界数据流：**有开始没结束，但是对于无界数据流来说我们无法判断何时所有数据已经到达，所以这种模式通常需要以特定 的顺序（例如事件发生的顺序），或者增加消息队列机制，以保证结果的完整性。



**有界数据流：**有界数据流有明确定义的开始和结束，可以在执行 任何计算之前通过获取所有数据来处理有界流，处理有界流不需要有序读取。目前我们的离线数据读取和计算工作就满足这一条件 。所以最终选择flink作为计算模块的核心框架。

FlinkML 基于Filnk平台的机器学习工具 使用Scala语言，但是FlinkML的机器学习论坛还很冷门，无法找到一些可以使用的资源。



- [x] **（2）Spark ML lib**

优势在于有丰富的ML论坛，可以提供简洁的API。为了更好的让机器学习与大数据框架结合，最终选择使用Spark大数据框架。


#### SparkML 决策树

（1）准备工作

安装Scala以及Spark本地版

这里选择的版本是最新的：Spark 3.2 + Scala 2.13.1 

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/Spark版本.png)

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/Scala版本.png)

解压到 /opt/module 文件夹下，命令：

```shell
sudo tar -zxvf /data/.../scala2.13.1或spark... -C /opt/module
```

（将解压后的Spark 文件夹改名为 spark-local，方便后续处理）

```shell
sudo mv /opt/module/spark2.13....  /opt/module/spark-local
```

配置Scala环境变量：

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/配置Scala环境变量.png)

```shell
source /etc/profile      //命令行生效
```

启动 Spark 

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/启动Spark.png)

命令： bin/spark-shell

可视化控制台：我们可以通过它来监测正在运行的程序健康状态。

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/spark可视化控制台.png)



目前依据现有的数据进行实验，主要选中了以下几个指标，来源于上述图中的前兆信息指标。

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/选中实验指标.png)

由于当前的分布式集群HBase迫于性能压力无法存储大量数据，所以只能暂时手动模拟数据进行测试。

思路分析：

![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/微震预警决策树模型.png)

**对于这张决策树示意图可能会有疑问，为什么是这样的顺序 即如何选择Cart决策树根节点字段的问题？**

我们的特征列往往由多个字段组成，特征列也就是后面决策树中的判定条件，那么每个节点中用于判定的字段的选择是如何决定的，这是值得研究的。

**选择一个合理的根节点字段决策树的分类效果会非常好，即每个叶子节点的输出会比较纯净。纯净度越高即选择的根节点字段就越合理。纯净度 也就是 信息增益、信息增益率和 gini 指数**。由于信息增益与信息增益率都只能对离散型数据进行分类，这是一个明显的短板，所以Breiman等人在1984年提出了CART算法，该算法也称为分类回归树，它所使用的字段选择指标是 **gini 指数法**。这个算法在程序中是可以自动实现的，当然也可以手动计算。具体算法以及流程已总结博客（地址：https://mp.weixin.qq.com/s?__biz=Mzg3OTI1ODkzOQ==&mid=2247485824&idx=2&sn=bf8b14024ff563d4bc3ab05b3239b044&chksm=cf0675d4f871fcc2c4be21fb7b3d3b2f6f086fc8904bb3cb3b434c5fedc290f41dc8ef5f84ed&token=1468615983&lang=zh_CN#rd）



数据准备：

（1）命名 tree1.txt 即训练集

字段说明：是否报警   总事件数  是否震级异常  是否B值异常  是否能量异常

```
1,6 1 1 0
1,8 1 1 0
0,3 1 1 1
0,4 0 1 0
0,5 0 0 0
0,6 0 1 1
1,7 1 0 1
1,6 1 1 1
1,5 1 0 0
1,8 1 0 1
```

命名 tree2.txt 即测试集

```
0,3 1 1 0
1,7 1 1 1
1,9 1 1 0
1,8 1 0 1
0,2 0 0 1
```



demo代码记录：

```scala
import org.apache.spark.mllib.feature.HashingTF
import org.apache.log4j.{Level,Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf,SparkContext}

/**
 * @description 功能描述
 * @author han56
 * @create 2021/12/27 上午9:03
 */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //训练数据
    val data1 = sc.textFile("/data/files/sparkMl/tree1.txt")

    //测试数据
    val data2 = sc.textFile("/data/files/sparkMl/tree2.txt")

    //转换向量
    val tree1 = data1.map{ line=>
      val  parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    val tree2 = data2.map{ line=>
      val  parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    //赋值
    val (trainningData,testData) = (tree1,tree2)

    //分类
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity = "gini"

    //最大深度
    val maxDepth = 5
    //最大分支
    val maxBins  = 32

    //模型训练
    val model = DecisionTree.trainClassifier(trainningData,numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    //模型预测
    val labelAndPreds = testData.map{point=>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }

    //测试值与真实值对比
    val print_predict = labelAndPreds.take(15)
    println("label"+"\t"+"prediction")
    for (i <- print_predict.indices){
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    //树的错误率
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble/testData.count()
    println("Test Error = "+testErr)
    //打印树的判断值
    println("Learned classification tree model:\n"+model.toDebugString)
  }
}
```

测试结果：

```json
label  prediction
0.0  0.0
1.0  1.0
1.0  1.0
1.0  1.0
0.0  0.0
Test Error = 0.0
Learned classification tree model:
```

后续集群升级后，会增加测试数据量，训练出更加完备的模型。



#### 主要参考论文

《机器学习预测实验室地震》 提供了随机森林/决策树进行预测的思想。

《两硬条件下冲击地压微震信号特征及前兆识别》 提供了可以采用 b值 和 能量 的特征计算前兆信号识别的思想。

 《煤矿动力灾害监测系统设计与实现》——陈博强【辽宁大学】提供了计算公式

**《Hbase架构中RPC客户端的通信性能优化》——谭良 提供了HBase写入优化的建议**

《Machine Learning in Apache Spark Environment for Diagnosis of Diabetes》

《Large scale data analysis using MLlib》

《微震信号识别与地压灾害微震前兆的研究》

《基于Spark和随机森林优化的糖尿病预测_杨雨含》

《基于实时测震数据的可视化系统的设计与实现》

《Ensemble_Pruning_of_RF_via_Multi-Objective_TLBO_Algorithm_and_Its_Parallelization_on_Spark》

《Evaluation of Static Vulnerability Detection Tools》

《Kafka-ML:ConnectingthedatastreamwithML⁄AIframeworks》

### 优化过程 ###

#### HBase存储速度优化

批量存储即 putList 形式的优化方式会**导致每一秒的数据的时间戳一致，这样就达不到  Versions 存储特性的使用**。

![](/data/files/杂项/研究生阶段/后台程序运行截图/存储模块/插入5000行控制台信息.png)

一次性插入5000条数据集群扛不住，经过多番测试以目前的配置，最快的状况是每次批量插入100条数据。

目前优化的思路有两个：
1.参考论文：《HBase架构中RPC客户端的通信性能优化》，将客户端改为基于RPC的非阻塞式通信。

2.参考博客：https://cloud.tencent.com/developer/article/1382924



参考论文关键点：

- 采用Java语言中最新的 NIO 机制实现 客户端《--》服务端 之间的非阻塞通信。
- 多线程并发执行。加入**线程池**可以有效的管理和调度工作中的线程。

##### JavaNIO + 多线程优化代码实现



其他相关论文研究：

![](/data/files/杂项/研究生阶段/论文/大数据论文/HBase存储优化研究/研究论文合集截图.png)

- 《Facility Information Management on HBase:Large-Scale Storage for Time-Series Data》

  面向物联网大数据基于HBase的数据管理。

  在本文中，重点讨论了HBase在设施信息管理中的应用，特别是从传感器或设施（如HVAC（供暖、通风和空调）、灯光控制系统、环境监测器和电能表）**收集的时间序列数据**。针对在云平台上管理数千栋建筑，推出一个开发FIAPStoragePeta的项目——计划“用于设施信息访问协议（FIAP）的PB级存储”。本文介绍了FIAPStor agePeta的体系结构和设计，提出了一种基于HBase的设备信息管理方案。

- 《Distributed Storage System for Electric Power Data Based on HBase》

  基于一个电力大数据的系统，**通过调整HBase参数进行的性能优化**

- 《Efficient Spatial Big Data Storage and Query in HBase》

  主要**通过Hilbert Curve算法对空间坐标大数据的存储与查询方面的优化**。

- 《Financial Big Data Hot and Cold Separation Scheme Based on HBase and Redis》基于HBase+Redis实现的金融大数据冷热数据区分模式。

  核心思想就是将 **经常被查询的数据称为 hot data 存在Redis中 把查询频率较小的数据称为 cold data 存在HBase中**。这样设计能够提高查询效率。

- 《面向时序大数据的数据库性能研究》

  该文章对关系型数据库、本地NoSQL数据库以及云NoSQL数据库在燃气大数据的应用场景下进行了定量定型的实验分析。实验结果表明，相比于关系型数据库，NoSQL数据库更加适合存储时序大数据。然后**提出了对不同场景下的时序大数据的数据库选型建议**。

- 《Data Cache Optimization Model Based on HBase and Redis》 

  基于Redis和HBase存储图片数据的优化

  这篇文章探讨了云存储高质量图片模型，提出了一种**基于HBase+Redis组合的缓存策略**。此外，该模型还改进了高性能缓存技术的缺陷，设置了存储在Hbase中的索引，实现了**索引与数据节点的映射**。

  

##### Netty+【多线程+线程池】+Redis缓存队列+HBase多Versions设计

1. *（Redis缓存机制）***在读取过程中**，模拟类似于操作系统中**“进程调度”**的思想（先来先服务/高响应比优先/最短作业优先/时间片轮转/最高优先级等）可以保证将**请求次数尽量的减小**，如果客户端请求的数据我此时的缓存里刚好存在，那么客户端就不用麻烦再去服务端请求这么一大圈弯路，直接返回请求的数据即可，而且我们的海量数据十分适合这样的机制，因为存储的各个字段都是带有时间戳的。**在本文中的读取部分主要是依照时间顺序的**，且读取的数据是为了接下来决策树指标计算做准备的，所以设计的机制就是在计算引擎正在计算时间段1的时候，开一个线程将时间段2的数据放在缓存中，这样可以使计算线程和读取线程始终保持在工作的状态，通俗的讲就是让他们都有活干，谁也别闲着。假如不这样设计的话 整个读取到计算的模块就是同步的，计算模块会一直等待读取模块拿来新的数据，而读取模块也会在给计算模块读完其所需要的数据之后陷入等待状态，效率极低。

2. *（Redis缓存机制）***在存储过程中**，可能会有这样的疑问，为什么要在这之间加入一个缓存队列？直接存进去不才是最快的。我一开始也有这样的疑问，后来成功说服了自己，按前者的说法有一个非常严重的bug，在如此巨量的数据的情况下，很难保证存储过程是可靠的，为了**增加存储整体过程的稳定性**，需要把这些待存的数据在路上就放进一个缓存队列里，**而且一定是 P2P 的**，**此时客户端是生产者，服务端是消费者**，生产者不断地向缓存队列中加入数据，消费者不断地接收队列中的数据，**稳定性就在于**一旦服务器挂掉了即存不进去数据了比如向上图的情况，缓存队列里依然保存着未被消费掉的数据，等到服务端恢复了，服务端就会自动去获取，继续一个一个的处理。速度上的降低也是基于已被优化的情况。而且由于设置的**缓存队列本质上就是内存**，对速度的影响是比较小的。

3. **HBase多Versions存储机制**：

   ![](/data/files/杂项/研究生阶段/后台程序运行截图/计算模块/前兆信息HBase标记.png)

   上图中粉色区域我们定义为一个分析计算时间段，分析时间段中包含若干时间戳，由于传感器平均一秒钟会产生5000条数据，且属于同一个历史时间戳，在该数据表设计模式下，一个历史时间戳就是一个HBase RowKey，那么这个问题就转换为在一个RowKey下存储多个Versions版本的数据，这也是HBase相较于其他传统数据库的优势之一，就是可以在一行中可以存储多条数据。具体分解图如下图所示：

   ![](/data/files/杂项/研究生阶段/后台程序运行截图/HBase存储优化部分/Hbase同一时间戳多versions结构.png)

   严格意义上，这种数据形式属于一种**极为特殊的时序数据**，因为参考的大多有关HBase存储/查询时序数据的文章中，他们的实验数据是**单位时间内一条数据的**，而我们的数据是**单位时间5000条**，这是一种前所未有的数据形式。但是这种数据事实上又是常见的比如物联网传感器频率很高的设备是很多的。所以以此为根据设计了以上形式的HBase数据存储模型。

   

   但是HBase的多Versions存储机制有一点很鸡肋，因为versions的插入需要时间戳的不同才能判定是两个不同的version数据，如果使用List<Put>批量插入的模式，这5000条versions数据会在同一个时间戳内被插入，最终显示的只能是最后被插入的那个versions数据。所以单纯的仅仅优化成批量put操作还是行不通，这种存储方式适用于插入一秒一条的数据即一个rowkey对应一条versions数据。

   

   为了解决这个问题。该算法模型加入了**自定义时间戳的设计**，这种设计模式在实际的业务场景下极为特殊才会使用，因为随着数据的增多，自定义时间戳会达到最大值，一旦达到最大值将会导致无法插入数据的风险。但是根据调查，如果从当前时间戳开始插入的话，实际上是不需要太担心的。因为时间戳TimeStamp在各类语言中都是Long类型的，而long的最大值为 922 3372 0368 5477 5807。而我们要搭建的离线数据仓库一秒钟是5000条数据，那么一天是 1800万 条，一个月最多是 5.5亿条，一年最多是70亿。

    

   新的问题：线程暂停的最小时间单位。Thread.sleep(1) 还能不能更小了

   

   存储算法模型代码如下所示：

   ```java
   public static void putRowsData(){
           try {
               //与HBase数据表建立连接
               Table table = connection.getTable(TableName.valueOf("test1"));
               //指定插入rowkey
               String rowKey = "rowkey2";
               List<Put> putList = new ArrayList<>();
               Put put = new Put(Bytes.toBytes(rowKey));
               for (int i=0;i<5;i++){
                  put.addColumn(Bytes.toBytes("fam1"),
                            Bytes.toBytes("name"),
                            //通过每一条插入的时间戳区分非同一个Verions
                            System.currentTimeMillis(),
                            Bytes.toBytes("branch"+i));
                   putList.add(put);
                   //关键部分 线程睡眠一毫秒，否则就会出现versions数据覆盖问题
                   Thread.sleep(1);
               }
               table.put(putList);
               table.close();
               System.out.println("批量插入成功");
           } catch (IOException | InterruptedException e) {
               e.printStackTrace();
           }
       }
   ```

4. **JavaAIO完全非阻塞异步存储技术：**

   即Asynchronous IO：异步非阻塞编程方式。
   
   与NIO不同，当进行操作时读写操作时，只需直接调用API的read/write方法即可。且这两种方法均是异步的，对于读操作而言，当有流可读时，操作系统会将可读的流传入read方法的缓冲区，并通知Application。对于写操作来说，当操作系统将write方法传递的流写入完毕时，OS主动通知Application。所以在AIO的操作下，读写均是异步的且完成后会主动调用回调函数。AIO主要在 java.nio.channels包下，增加了如下四个异步通道。
   
   - AsynchronousSocketChannel
   - AsynchronousServerSocketChannel 
   - AsynchronousFileChannel
   - AsynchronousDatagramChannel 
   
   同时加入多线程+线程池的思想，设置 n 个核心线程，n代表用几个文件参与计算，假设 n==6; 此时就可以通过这六个线程异步地向HBase存储数据。这部分将会与传统的NIO BIO通信方式做对比实验





以上的优化都是针对客户端的，主要问题在于 经过JMeter性能极限测试，客户端是可以向服务端发送巨大数据的，服务端却无法处理这些数据，导致整个存储过程拥塞。

**服务端优化**非常简单，加大内存、提高带宽、增加集群服务器CPU数量、调整参数等硬件因素即可实现速度的飞跃。

