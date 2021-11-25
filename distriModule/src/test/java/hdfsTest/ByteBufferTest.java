package hdfsTest;

import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/11/19 上午8:47
 */
public class ByteBufferTest {

    /*
    * 打印当前指针位置，缓冲区容量，缓冲区限制最大位数信息
    * */
    private void printByteByfer(ByteBuffer buffer){
        System.out.println("position:"+buffer.position()+"  "+
                "capacity:"+buffer.capacity()+"  "+"limit:"+buffer.limit());
    }

    /*
    *
    * */
    private void printDelimiter(ByteBuffer buffer){
        int newDelimiter = buffer.getInt();
        System.out.println("delimiter:"+Integer.toHexString(newDelimiter));
        printByteByfer(buffer);
    }

    private void printLength(ByteBuffer buffer){
        int length = buffer.getInt();
        System.out.println("length:"+length);
        printByteByfer(buffer);
    }

    @Test
    public void byteBufferTest(){
        /*
        * 写入方法体
        * */
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        byteBuffer.putInt(0xabef0101);
        byteBuffer.putInt(1024);
        byteBuffer.put((byte) 1);
        byteBuffer.put((byte) 0);

        /*
        * 因为写完后position已经到10了，所以需要通过这个方法将指针跳回0的位置
        * 再从头读取
        * */
        byteBuffer.flip();
        System.out.println("flip翻转后数据的position位置信息打印");
        printDelimiter(byteBuffer);

        //读取 length
        printLength(byteBuffer);

        //继续读取剩下数据
        byteBuffer.get();
        byteBuffer.get();
        printByteByfer(byteBuffer);

        //再次jump回0，还是可以重头读
        byteBuffer.flip();
        printDelimiter(byteBuffer);

        //clear清空一下，在重头开始读
        byteBuffer.clear();
        printDelimiter(byteBuffer);

        //rewind一下
        byteBuffer.rewind();
        printDelimiter(byteBuffer);

        //mark标记
        byteBuffer.mark();

        //再读取下面四个字节Int类型
        printLength(byteBuffer);
        printByteByfer(byteBuffer);

        //reset重置，回到mark标记的位置
        byteBuffer.reset();
        printByteByfer(byteBuffer);

    }

    @Test
    public void getStartAndEndTime(){
        List<String> filesPath = new ArrayList<>();
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/S/Test_190925110520.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/Z/Test_190925103745.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/U/Test_190925105915.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/V/Test_190925100949.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/Y/Test_190925101650.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/T/Test_190925104506.HFMED");
        List<String> dateArray = new ArrayList<>();
        /*
         * 获取文件路径中的起始时间与终止时间
         * */
        for (String str:filesPath){
            String timeStr = "20"+str.substring(55,57)+"-"+str.substring(57,59)+"-"+str.substring(59,61)
                    +" "+str.substring(61,63)+":"+str.substring(63,65)+":"+str.substring(65,67);
            dateArray.add(timeStr);
        }
        System.out.println(dateArray);
        Map<String, Integer> dateMap = new TreeMap<>();
        for(String dateKey:dateArray){
            if(dateMap.containsKey(dateKey)){
                int value = dateMap.get(dateKey) + 1;
                dateMap.put(dateKey, value);
            }else{
                dateMap.put(dateKey, 1);
            }
        }
        Set<String> keySet = dateMap.keySet();
        String []sorttedArray = new String[keySet.size()];
        Iterator<String> iter = keySet.iterator();
        int index = 0;
        while (iter.hasNext()) {
            String key = iter.next();
            sorttedArray[index++] = key;
        }
        int sorttedArrayLen = sorttedArray.length;
        System.out.println("最小日期是：" + sorttedArray[0]);
        System.out.println("最大日期是：" + sorttedArray[sorttedArrayLen - 1]);

        //即可推出整个数据读取期间的时间窗口
        String windowStartTime = sorttedArray[sorttedArrayLen - 1];
        String windowEndTime = addDateHour(sorttedArray[0], 1);
        System.out.println("滑动窗口起始时间："+windowStartTime);
        System.out.println("滑动窗口结束时间："+windowEndTime);
    }

    //返回的是字符串型的时间，输入的
    //是String day, int x
    public static String addDateHour(String day, int x){

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 24小时制
        //引号里面个格式也可以是 HH:mm:ss或者HH:mm等等，很随意的，不过在主函数调用时，要和输入的变
        //量day格式一致
        Date date = null;
        try {
            date = format.parse(day);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (date == null)
            return "";
        System.out.println("front:" + format.format(date)); //显示输入的日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, x);// 24小时制
        date = cal.getTime();
        System.out.println("after:" + format.format(date));  //显示更新后的日期
        return format.format(date);
    }

}
