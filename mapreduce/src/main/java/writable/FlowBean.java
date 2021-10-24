package writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author han56
 * @description 功能描述 上下行流量Bean类
 * @create 2021/9/16 下午4:02
 * 1.定义类实现Writable接口
 * 2.重写序列化和反序列化接口方法
 * 3.重写空参数构造
 * 3.重写toString方法，用于打印输出
 */

public class FlowBean implements Writable {

    private long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow;//总流量
    /*
    * 空参数构造
    * */

    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    //重载总流量方法
    public void setSumFlow() {
        this.sumFlow = this.upFlow+this.downFlow;
    }

    //重写序列化方法
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    /*
     *注意：序列化的顺序一旦确定，反序列化的顺序必须与之对应
     * */

    //重写反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    //重写 toString 方法
    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }
}
