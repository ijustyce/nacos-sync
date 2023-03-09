package com.alibaba.nacossync.util;

import com.ecwid.consul.transport.DefaultHttpTransport;
import com.ecwid.consul.transport.DefaultHttpsTransport;
import com.ecwid.consul.transport.HttpRequest;
import com.ecwid.consul.transport.HttpTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

@Slf4j
public class AlarmUtil {

    private static final String ROBOT_URL =  "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=892c0289-6479-4714-9cc6-d9598d2f671b";

    private static final String ALARM_CONTENT = "{" +
            "    \"msgtype\": \"text\"," +
            "    \"text\": {" +
            "        \"content\": \"%s\"" +
            "    }" +
            "}";

    public static void alarm(String msg) {
        if (StringUtils.isEmpty(msg)) {
            return;
        }
        String env = StringUtils.isBlank(System.getProperty("spring.profiles.active")) ? "dev" : System.getProperty("spring.profiles.active");
        String prefix = "[" + env + "]-";
        try {
            String content = String.format(ALARM_CONTENT, prefix +  msg);
            CloseableHttpClient client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(ROBOT_URL);
            StringEntity entity = new StringEntity(content, "utf-8");

            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            post.setEntity(entity);
            client.execute(post,re->{
                System.out.println(re);
                return re;
                    }
            );

        } catch (Exception e) {
            log.error("微信告警发送失败" + msg, e);
        }
    }
    public static void main(String[] args) {
        alarm("test");
    }
}


