package com.atguigu.flume.interceptor.db;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

//import java.nio.charset.StandardCharsets;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> header = event.getHeaders();
        byte[] body = event.getBody();
        JSONObject jsonObject = JSONObject.parseObject(new String(body, StandardCharsets.UTF_8));
        Long ts = jsonObject.getLong("ts");
        header.put("timestamp",String.valueOf(ts*1000));
        event.setHeaders(header);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event e : list){
            intercept(e);
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
