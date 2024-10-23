package crm.middleware.project.sdk.http.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.rkhd.platform.sdk.exception.XsyHttpException;
import com.rkhd.platform.sdk.http.CommonData;

import crm.middleware.project.sdk.http.config.CrmApiUrl;
import crm.middleware.project.sdk.http.config.CrmConfig;
import crm.middleware.project.sdk.http.config.CrmTokens;
import crm.middleware.project.sdk.http.rest.http.CommonHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public final class RestClient {

    private static final int DEFAULT_SOCKET_TIMEOUT = 30000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 30000;
    private static final String CHARSET = "utf-8";

    private static final RestClient INSTANCE = new RestClient();
    private static SSLConnectionSocketFactory sslConnectionSocketFactory;
    private static RequestConfig config;

    private static CommonHttpClient client = null;
    private static int retry = 0;
    private static int retry_get = 0;
    private static int retry_post_str = 0;
    private static int retry_post_map = 0;
    private static int retry_patch = 0;
    private static int retry_delete = 0;
    private static int retry_put = 0;

    public static RestClient getInstance() {
        return INSTANCE;
    }

    private RestClient() {
        initTrustHosts();
        initConfig();
    }

    private void initConfig() {
        config = RequestConfig.custom().setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT).setSocketTimeout(DEFAULT_SOCKET_TIMEOUT).build();
    }

    private void initTrustHosts() {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                //信任所有
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
            sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        }
    }

    public CloseableHttpClient createClient() {
        return HttpClients.custom().setDefaultRequestConfig(config).setSSLSocketFactory(sslConnectionSocketFactory).build();
    }

    public JSONObject get(String url, Map<String, String> params) {
        url = url.trim();
        StringBuilder sb = new StringBuilder(url);
        JSONObject resultJson = new JSONObject();
        String result = null;
        HttpGet httpGet = null;
        try {
            if (params != null && !params.isEmpty()) {
                List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    String value = entry.getValue();
                    if (value != null) {
                        pairs.add(new BasicNameValuePair(entry.getKey(), value));
                    }
                }
                sb.append("?").append(EntityUtils.toString(new UrlEncodedFormEntity(pairs, CHARSET)));
            }
            //String accessToken = getToken(client, "xsyAdmin");
            httpGet = new HttpGet(sb.toString());
           /* if (StringUtils.isNotBlank(accessToken)) {
                httpGet.addHeader("Authorization", "Bearer " + accessToken);
            }*/

            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpGet));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("get method >>> Return result(HttpEntity) is null");
                }
            } else {
                httpGet.abort();
                log.error("response result : " + JSONObject.toJSONString(response));
                throw new XsyHttpException("get method Return result status not 200(status=" + statusCode + ")");
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("get method Return result is error." + result + "\n httpGet params: " + JSON.toJSONString(httpGet));
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("get method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_get == 0) {
                            retry_get++;
                            get(url, params);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("get method Return result is error." + result);
            }
        } catch (Exception e) {
            log.error("httpGet params: " + JSON.toJSONString(httpGet));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "get method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_get = 0;
        }
        return resultJson;
    }

    public static JSONObject post(String url) {
        url = url.trim();
        StringEntity stringEntity = null;
        String result = null;
        JSONObject resultJson = new JSONObject();
        HttpPost httpPost = new HttpPost(url);
        try {
            String accessToken = getToken(client, "xsyAdmin");
            if (StringUtils.isNotBlank(accessToken)) {
                httpPost.addHeader("Authorization", "Bearer " + accessToken);
            }
            if (stringEntity != null) {
                httpPost.setEntity(stringEntity);
            }

            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpPost));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("post method >>> Return result(HttpEntity) is null");
                }
            } else {
                httpPost.abort();
                log.error("response result : " + JSONObject.toJSONString(response));
                throw new XsyHttpException("post method Return result status not 200(status=" + statusCode + ")");
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("post method Return result is error." + result);
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("post method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_post_str == 0) {
                            retry_post_str++;
                            post(url);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("post method Return result is " + result);
            }
        } catch (Exception e) {
            log.error("httpPost params: " + JSON.toJSONString(httpPost));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "post method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_post_str = 0;
        }
        return resultJson;
    }

    public static JSONObject post(String url, String jsonObject) {
        url = url.trim();
        StringEntity stringEntity = null;
        String result = null;
        JSONObject resultJson = new JSONObject();
        if (jsonObject != null) {
            stringEntity = new StringEntity(jsonObject.toString(), ContentType.APPLICATION_JSON.withCharset("utf-8"));
        }
        HttpPost httpPost = new HttpPost(url);
        try {
            String accessToken = getToken(client, "xsyAdmin");
            if (StringUtils.isNotBlank(accessToken)) {
                httpPost.addHeader("Authorization", "Bearer " + accessToken);
            }
            if (stringEntity != null) {
                httpPost.setEntity(stringEntity);
            }

            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpPost));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("post method >>> Return result(HttpEntity) is null");
                }
            } else {
                httpPost.abort();
                log.error("response result : " + JSONObject.toJSONString(response));
                throw new XsyHttpException("post method Return result status not 200(status=" + statusCode + ")");
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("post method Return result is error." + result);
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("post method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_post_str == 0) {
                            retry_post_str++;
                            post(url, jsonObject);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("post method Return result is " + result);
            }
        } catch (Exception e) {
            log.error("httpPost params: " + JSON.toJSONString(httpPost));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "post method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_post_str = 0;
        }
        return resultJson;
    }

    public JSONObject post(String url, Map<String, String> params) {
        UrlEncodedFormEntity urlEncodedFormEntity = null;
        String result = null;
        JSONObject resultJson = new JSONObject();
        HttpPost httpPost = null;
        try {
            if (params != null && !params.isEmpty()) {
                List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    String value = entry.getValue();
                    if (value != null) {
                        pairs.add(new BasicNameValuePair(entry.getKey(), value));
                    }
                }
                urlEncodedFormEntity = new UrlEncodedFormEntity(pairs, CHARSET);
            }
            httpPost = new HttpPost(url);
            String accessToken = getToken(client, "xsyAdmin");
            if (StringUtils.isNotBlank(accessToken)) {
                httpPost.addHeader("Authorization", "Bearer " + accessToken);
            }
            if (urlEncodedFormEntity != null) {
                httpPost.setEntity(urlEncodedFormEntity);
            }
            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpPost));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("post method >>> Return result(HttpEntity) is null..");
                }
            } else {
                httpPost.abort();
                log.error("response result：" + JSON.toJSONString(response));
                throw new XsyHttpException("post method Return result status not 200(status=" + statusCode);
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("post method Return result is error." + result);
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("post method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_post_map == 0) {
                            retry_post_map++;
                            post(url, params);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("post method Return result is " + result);
            }
        } catch (Exception e) {
            log.error("httpPost params: " + JSON.toJSONString(httpPost));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "post method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_post_map = 0;
        }
        return resultJson;
    }


    public JSONObject patch(String url, String jsonObject) {
        url = url.trim();
        StringEntity stringEntity = null;
        String result = null;
        JSONObject resultJson = new JSONObject();
        if (jsonObject != null) {
            stringEntity = new StringEntity(jsonObject.toString(), ContentType.APPLICATION_JSON.withCharset("utf-8"));
        }
        HttpPatch httpPatch = new HttpPatch(url);
        try {
            String accessToken = getToken(client, "xsyAdmin");
            if (StringUtils.isNotBlank(accessToken)) {
                httpPatch.addHeader("Authorization", "Bearer " + accessToken);
            }
            if (stringEntity != null) {
                httpPatch.setEntity(stringEntity);
            }

            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpPatch);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpPatch));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("patch method >>> Return result(HttpEntity) is null.");
                }
            } else {
                httpPatch.abort();
                log.error("reponse result : " + JSONObject.toJSONString(response));
                throw new XsyHttpException("patch method Return result status not 200(status=" + statusCode);
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("patch method Return result is error." + result);
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("patch method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_patch == 0) {
                            retry_patch++;
                            patch(url, jsonObject);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("post method Return result is " + result);
            }
        } catch (Exception e) {
            log.error("httpPatch params: " + JSON.toJSONString(httpPatch));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "patch method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_patch = 0;
        }
        return resultJson;
    }

    public JSONObject delete(String url) {
        url = url.trim();
        StringEntity stringEntity = null;
        String result = null;
        JSONObject resultJson = new JSONObject();
        HttpDelete httpDelete = new HttpDelete(url);
        try {
            String accessToken = getToken(client, "xsyAdmin");
            if (StringUtils.isNotBlank(accessToken)) {
                httpDelete.addHeader("Authorization", "Bearer " + accessToken);
            }

            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpDelete);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpDelete));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("delete method >>> Return result(HttpEntity) is null.");
                }
            } else {
                httpDelete.abort();
                log.error("response result : " + JSONObject.toJSONString(response));
                throw new XsyHttpException("delete method Return result status not 200(status=" + statusCode);
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("delete method Return result is error." + result);
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("delete method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_delete == 0) {
                            retry_delete++;
                            delete(url);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("post method Return result is " + result);
            }
        } catch (Exception e) {
            log.error("httpDelete params: " + JSON.toJSONString(httpDelete));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "delete method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_delete = 0;
        }
        return resultJson;
    }

    public JSONObject put(String url, String jsonObject) {
        url = url.trim();
        StringEntity stringEntity = null;
        String result = null;
        JSONObject resultJson = new JSONObject();
        if (jsonObject != null) {
            stringEntity = new StringEntity(jsonObject.toString(), ContentType.APPLICATION_JSON.withCharset("utf-8"));
        }
        HttpPut httpPut = new HttpPut(url);
        try {
            String accessToken = getToken(client, "xsyAdmin");
            if (StringUtils.isNotBlank(accessToken)) {
                httpPut.addHeader("Authorization", "Bearer " + accessToken);
            }
            if (stringEntity != null) {
                httpPut.setEntity(stringEntity);
            }

            CloseableHttpResponse response = RestClient.getInstance().createClient().execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity, CHARSET);
                    log.info("result >>> " + result + ";params: " + JSON.toJSONString(httpPut));
                    EntityUtils.consume(entity);
                } else {
                    throw new XsyHttpException("put method >>> Return result(HttpEntity) is null.");
                }
            } else {
                httpPut.abort();
                log.error("response result " + JSONObject.toJSONString(response));
                throw new XsyHttpException("put method Return result status not 200(status=" + statusCode);
            }
            response.close();
            if (StringUtils.isNotBlank(result)) {
                resultJson = JSONObject.parseObject(result);
                if (result.contains("error_code")) {
                    log.error("put method Return result is error." + result);
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("put method Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("xsyAdmin");
                        if (retry_put == 0) {
                            retry_put++;
                            put(url, jsonObject);
                        }
                    }
                }
            } else {
                throw new XsyHttpException("post method Return result is " + result);
            }
        } catch (Exception e) {
            log.error("httpPut params: " + JSON.toJSONString(httpPut));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "put method System exception..";
            }
            resultJson.put("error_code", "555005");
            resultJson.put("message", result);
        } finally {
            retry_put = 0;
        }
        return resultJson;
    }

    public static JSONObject commonHttpClient(String url, Map<String, String> formData, JSONObject bodyData,
                                              String requestType, String contentType) {
        // if (client == null)
        CommonHttpClient client = new CommonHttpClient();
        JSONObject resultJson = new JSONObject();
        String result = null;
        if (contentType != null)
            client.setContentType(contentType);

        CommonData commonData = new CommonData();
        try {
            commonData.setCallString(url);
            commonData.setCall_type(requestType);
            commonData.addHeader("Authorization", "Bearer " + getToken(client, "crmToken"));
            if (formData != null) {
                for (Map.Entry<String, String> entry : formData.entrySet()) {
                    String value = entry.getValue();
                    if (value != null) {
                        commonData.putFormData(entry.getKey(), value);
                    }
                }
            }
            if (bodyData != null)
                //commonData.setBody(JSON.toJSONString(bodyData));
                commonData.setBody(JSON.toJSONString(bodyData, SerializerFeature.WriteMapNullValue));

            for (int i = 0; i < 2; i++) {
                result = client.performRequest(commonData);
                //log.info(getMethodAndClass(new Exception().getStackTrace()) + "result >>> (" + i + ") = " + result + ";params: " + JSON.toJSONString(commonData));
                if (StringUtils.isNotBlank(result)) {
                    break;
                } else {
                    Thread.sleep(500);
                }
            }
            //log.info("http返回结果："+result);
            resultJson = JSONObject.parseObject(result);
            if (url.contains("v1")) {
                if (result.contains("error_code")) {
                    //log.error("crm sdk performRequest() >>> Return result is error." + result + " ;\n params: " + JSONObject.toJSON(commonData));
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("crmToken");
                        if (retry == 0) {
                            retry++;
                            commonHttpClient(url, formData, bodyData, requestType, contentType);
                        }
                    }
                }
            } else {
                if (resultJson.getLongValue("code") != 200L) {
                    //log.error("crm sdk performRequest() >>> Return result is error." + result + " ;\n params: " + JSONObject.toJSON(commonData));
                    if ((resultJson.get("code").toString()).equals("1020008")) {
                        log.error("Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("crmToken");
                        if (retry == 0) {
                            retry++;
                            commonHttpClient(url, formData, bodyData, requestType, contentType);
                        }
                    }
                } else {
                    if (resultJson.containsKey("date")) {
                        resultJson = resultJson.getJSONObject("date");
                    }
                    if (resultJson.containsKey("result")) {
                        try {
                            resultJson = resultJson.getJSONObject("result");
                        } catch (ClassCastException cce) {
                            JSONArray arry = resultJson.getJSONArray("result");
                            JSONObject newObj = new JSONObject();
                            newObj.put("result", arry);
                            resultJson = newObj;
                        }
                    }
                }
            }
        } catch (Exception e) {
            //log.error("performRequest() exception; params: " + JSON.toJSONString(commonData));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "performRequest() method System exception..";
            }

            if (url.contains("v2")) {
                resultJson.put("code", "555005");
            } else {
                resultJson.put("error_code", "555005");
            }
            resultJson.put("message", result);
            //throw new XsyHttpException(result);
        } finally {
            retry = 0;
        }
        return resultJson;
    }

    public static JSONObject commonHttpClient_Object(String url, Map<String, Object> formData, JSONObject bodyData,
                                              String requestType, String contentType) {
        // if (client == null)
        CommonHttpClient client = new CommonHttpClient();
        JSONObject resultJson = new JSONObject();
        String result = null;
        if (contentType != null)
            client.setContentType(contentType);

        CommonData commonData = new CommonData();
        try {
            commonData.setCallString(url);
            commonData.setCall_type(requestType);
            commonData.addHeader("Authorization", "Bearer " + getToken(client, "crmToken"));
            if (formData != null) {
                for (Map.Entry<String, Object> entry : formData.entrySet()) {
                    Object value = entry.getValue();
                    if (value != null) {
                        commonData.putFormData(entry.getKey(), value);
                    }
                }
            }
            if (bodyData != null)
                commonData.setBody(JSON.toJSONString(bodyData));

            for (int i = 0; i < 2; i++) {
                result = client.performRequest(commonData);
                //log.info(getMethodAndClass(new Exception().getStackTrace()) + "result >>> (" + i + ") = " + result + ";params: " + JSON.toJSONString(commonData));
                if (StringUtils.isNotBlank(result)) {
                    break;
                } else {
                    Thread.sleep(500);
                }
            }
            log.info("http返回结果："+result);
            resultJson = JSONObject.parseObject(result);
            if (url.contains("v1")) {
                if (result.contains("error_code")) {
                    log.error("crm sdk performRequest() >>> Return result is error." + result + " ;\n params: " + JSONObject.toJSON(commonData));
                    if (resultJson.get("error_code").toString().equals("20000002")) {
                        log.error("Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("crmToken");
                        if (retry == 0) {
                            retry++;
                            commonHttpClient_Object(url, formData, bodyData, requestType, contentType);
                        }
                    }
                }
            } else {
                if (resultJson.getLongValue("code") != 200L) {
                    log.error("crm sdk performRequest() >>> Return result is error." + result + " ;\n params: " + JSONObject.toJSON(commonData));
                    if (resultJson.getLongValue("code") == 1020008L) {
                        log.error("Token information error ：" + resultJson.toJSONString());
                        CrmTokens.removeToken("crmToken");
                        if (retry == 0) {
                            retry++;
                            commonHttpClient_Object(url, formData, bodyData, requestType, contentType);
                        }
                    }
                } else {
                    if (resultJson.containsKey("date")) {
                        resultJson = resultJson.getJSONObject("date");
                    }
                    if (resultJson.containsKey("result")) {
                        try {
                            resultJson = resultJson.getJSONObject("result");
                        } catch (ClassCastException cce) {
                            JSONArray arry = resultJson.getJSONArray("result");
                            JSONObject newObj = new JSONObject();
                            newObj.put("result", arry);
                            resultJson = newObj;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("performRequest() exception; params: " + JSON.toJSONString(commonData));
            e.printStackTrace();
            if (e instanceof XsyHttpException) {
                result = e.getMessage().toString();
            } else {
                result = "performRequest() method System exception..";
            }

            if (url.contains("v2")) {
                resultJson.put("code", "555005");
            } else {
                resultJson.put("error_code", "555005");
            }
            resultJson.put("message", result);
            //throw new XsyHttpException(result);
        } finally {
            retry = 0;
        }
        return resultJson;
    }

    public static String getToken(CommonHttpClient client, String key) throws XsyHttpException {
        String token = CrmTokens.getToken(key);
        if (StringUtils.isBlank(token)) {
            if (client == null)
                client = new CommonHttpClient();
            CommonData param = new CommonData();
            param.setCall_type("POST");
            param.setCallString(CrmApiUrl.url_v1_oauth);
            param.putFormData("grant_type", "password");
            param.putFormData("client_id", CrmConfig.CLIENT_ID);
            param.putFormData("client_secret", CrmConfig.CLIENT_SECRET);
            param.putFormData("redirect_uri", CrmConfig.REDIRECT_URI);
            param.putFormData("username", CrmConfig.USER_NAME);
            param.putFormData("password", CrmConfig.PASSWORD + CrmConfig.SECURITY);
            String resulttoken = client.performRequest(param);
           // log.info("resulttoken:" + resulttoken);
            if (!StringUtils.isNotBlank(resulttoken)) {
                log.error("gettoken params: " + JSON.toJSONString(param));
                throw new XsyHttpException("getToken Return result is " + resulttoken);
            }
            JSONObject resultJson = JSONObject.parseObject(resulttoken);
            if (!resultJson.containsKey("access_token")) {
                log.error("gettoken params: " + JSON.toJSONString(param));
                throw new XsyHttpException("getToken Return result is " + resultJson.toJSONString());
            }
            token = resultJson.getString("access_token");
            CrmTokens.setToken(key, token);
           // log.info("token: " + token);
        }
        return token;
    }


    public static String getToken2(String key) throws XsyHttpException {

        try {

            String token = CrmTokens.getToken(key);
            log.info("token 2 -->" + token);
            if (StringUtils.isBlank(token)) {

                if (client == null)
                    client = new CommonHttpClient();

                CommonData param = new CommonData();
                param.setCall_type("POST");
                param.setCallString(CrmApiUrl.url_v1_oauth);
                param.putFormData("grant_type", "password");
                param.putFormData("client_id", CrmConfig.CLIENT_ID);
                param.putFormData("client_secret", CrmConfig.CLIENT_SECRET);
                param.putFormData("redirect_uri", CrmConfig.REDIRECT_URI);
                param.putFormData("username", CrmConfig.USER_NAME);
                param.putFormData("password", CrmConfig.PASSWORD + CrmConfig.SECURITY);
                String resulttoken = client.performRequest(param);
                log.info("resulttoken -->" + resulttoken);
                if (!StringUtils.isNotBlank(resulttoken)) {
                    throw new XsyHttpException("getToken Return result is " + resulttoken);
                }
                JSONObject tokenJson = JSONObject.parseObject(resulttoken);
                if (!tokenJson.containsKey("access_token")) {
                    throw new XsyHttpException("getToken Return result is " + tokenJson.toJSONString());
                }
                token = tokenJson.getString("access_token");
                CrmTokens.setToken(key, token);
                log.info("token: " + token);

                return token;
            }

        } catch (Exception e) {
            log.error("调用token异常：", e);

        }


        return null;
    }

    public static String getToken(String key) throws XsyHttpException {

        String token = CrmTokens.getToken(key);
        if (StringUtils.isBlank(token)) {
            if (client == null)
                client = new CommonHttpClient();
            CommonData param = null;
            try {
                param = new CommonData();
                param.setCall_type("POST");
                param.setCallString(CrmApiUrl.url_v1_oauth);
                param.putFormData("grant_type", "password");
                param.putFormData("client_id", CrmConfig.CLIENT_ID);
                param.putFormData("client_secret", CrmConfig.CLIENT_SECRET);
                param.putFormData("redirect_uri", CrmConfig.REDIRECT_URI);
                param.putFormData("username", CrmConfig.USER_NAME);
                param.putFormData("password", CrmConfig.PASSWORD + CrmConfig.SECURITY);
                String resulttoken = client.performRequest(param);
                if (!StringUtils.isNotBlank(resulttoken)) {
                    throw new XsyHttpException("getToken Return result is " + resulttoken);
                }
                JSONObject tokenJson = JSONObject.parseObject(resulttoken);
                if (!tokenJson.containsKey("access_token")) {
                    throw new XsyHttpException("getToken Return result is " + tokenJson.toJSONString());
                }
                token = tokenJson.getString("access_token");
                CrmTokens.setToken(key, token);
                log.info("token: " + token);
            } catch (Exception e) {
                //log.error("get token is error", e);
                log.error("gettoken params: " + JSON.toJSONString(param));
                e.printStackTrace();
                /*token = null;
                resultJson.put("error_code", "555005");
                resultJson.put("message", e.getMessage().toString());*/
                if (e instanceof XsyHttpException)
                    throw new XsyHttpException(e.getMessage().toString());
                else
                    throw new XsyHttpException("getToken() method System exception...");
            }
        }
        return token;
    }

    private static String getMethodAndClass(StackTraceElement[] classArray) {
        String methodAndClass = null;
        try {
            for (int i = 0; i < classArray.length; i++) {
                if (i == 1) {
                    String classname = classArray[i].getClassName();
                    String methodname = classArray[i].getMethodName();
                    methodAndClass = "【" + classname + "." + methodname + "】 ";
                    break;
                }
                //log.info(methodAndClass);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return methodAndClass;
    }


}