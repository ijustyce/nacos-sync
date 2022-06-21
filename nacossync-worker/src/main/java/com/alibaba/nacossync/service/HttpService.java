package com.alibaba.nacossync.service;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author 杨春 At 2022-06-21 15:50
 */

@Service
@Slf4j
public class HttpService {

    private final OkHttpClient okHttpClient;

    public HttpService() {
        this.okHttpClient = new OkHttpClient().newBuilder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    private String executeRequest(String nacosServer, Request.Builder builder, HttpUrl.Builder httpUrl) {
        if (ObjectUtils.isEmpty(nacosServer)) {
            throw new RuntimeException("nacosServer is empty.");
        }
        try {
            Request request = builder.url(nacosServer).build();
            String url = request.url().toString();
            Response response = okHttpClient.newCall(request).execute();
            ResponseBody responseBody = response.body();
            if (response.isSuccessful()) {
                String responseString = responseBody == null ? null : responseBody.string();
                if (responseString == null) {
                    log.error("responseString is null url is {}", httpUrl);
                }
                if (responseBody != null) {
                    responseBody.close();
                }
                log.info("request url is {}, result is {}", url, responseString);
                return responseString;
            } else {
                log.error("failed to send request url {}", url);
                if (responseBody != null) {
                    responseBody.close();
                }
                return null;
            }
        } catch (Exception e) {
            log.error("http request to {} failed", nacosServer, e);
        }
        return null;
    }
    
    public String httpPost(String nacosServer, String url, String json) {
        okhttp3.RequestBody requestBody = null;
        if (StringUtils.hasText(json)) {
            requestBody = okhttp3.RequestBody
                    .create(MediaType.parse("application/json; charset=utf-8"), json);
        }
        return httpPost(nacosServer, url, requestBody);
    }
    
    public String httpPost(String nacosServer, String url, RequestBody requestBody) {
        Request.Builder builder = new Request.Builder().method("POST", requestBody);
        HttpUrl.Builder httpUrl = new HttpUrl.Builder().encodedPath(url);
        return executeRequest(nacosServer, builder, httpUrl);
    }

    public String httpPut(String nacosServer, String url, String json) {
        okhttp3.RequestBody requestBody = okhttp3.RequestBody
                .create(MediaType.parse("application/json; charset=utf-8"), json);
        return httpPut(nacosServer, url, requestBody);
    }

    public String httpPut(String nacosServer, String url, RequestBody requestBody) {
        Request.Builder builder = new Request.Builder().put(requestBody);
        HttpUrl.Builder httpUrl = new HttpUrl.Builder().encodedPath(url);
        return executeRequest(nacosServer, builder, httpUrl);
    }

    /**
     * 以下为 get 和 delete 的实现
     */

    private HttpUrl.Builder httpBuilder(String url, HashMap<String, String> params) {
        HttpUrl.Builder httpBuilder = new HttpUrl.Builder().encodedPath(url);
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                String value = entry.getValue();
                if (value != null) {
                    value = value.trim();
                }
                httpBuilder.addQueryParameter(entry.getKey(), value);
            }
        }
        return httpBuilder;
    }

    public String executeRequest(String nacosServer, Request.Builder builder, String url,
                                               HashMap<String, String> params) {
        HttpUrl.Builder httpBuilder;
        try {
            httpBuilder = httpBuilder(url, params);
        } catch (Exception e) {
            log.error("error while request", e);
            return null;
        }
        return executeRequest(nacosServer, builder, httpBuilder);
    }

    public String httpGet(String nacosServer, String url, HashMap<String, String> params) {
        return executeRequest(nacosServer, new Request.Builder().get(), url, params);
    }

    public String httpDelete(String nacosServer, String url, HashMap<String, String> params) {
        return executeRequest(nacosServer, new Request.Builder().delete(), url, params);
    }

    public String httpDelete(String nacosServer, String url, String json, HashMap<String, String> params) {
        okhttp3.RequestBody requestBody = okhttp3.RequestBody
                .create(MediaType.parse("application/json; charset=utf-8"), json);
        return executeRequest(nacosServer, new Request.Builder().delete(requestBody), url, params);
    }

    public String httpGet(String url) {
        Request.Builder builder = new Request.Builder().get();
        Request request = builder.url(url).build();
        try {
            Response response = okHttpClient.newCall(request).execute();
            ResponseBody responseBody = response.body();
            String result = null;
            if (response.isSuccessful()) {
                result = responseBody == null ? null : responseBody.string();
            }
            if (responseBody != null) {
                responseBody.close();
            }
            return result;
        } catch (Exception e) {
            log.error("error while httpGet", e);
        }
        return null;
    }
}
