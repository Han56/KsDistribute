package com.example.httpcommutest.api;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * @author han56
 * @description 功能描述
 * @create 2022/7/7 下午2:28
 */
@RestController
public class ApiTest {

    //用于测试 HttpUrlConnection HttpClient OkHttp 性能接口
    @PutMapping("/put")
    @CrossOrigin
    public Object putTest(String content){
        //模拟处理事务 线程暂停若干秒
        try {
            Thread.sleep(1000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        String res = "copy that "+content;
        HashMap<String,String> map = new HashMap<>();
        map.put("res",res);
        return map;
    }

}
