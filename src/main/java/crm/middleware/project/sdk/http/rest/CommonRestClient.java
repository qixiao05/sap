package crm.middleware.project.sdk.http.rest;


import com.alibaba.fastjson.JSONObject;
import com.rkhd.platform.sdk.http.CommonData;
import com.rkhd.platform.sdk.http.CommonHttpClient;
import crm.middleware.project.cfg.CrmPropertiesConfig;
import crm.middleware.project.service.EatonApiImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.jar.JarEntry;


/**
 * 这个类 是对Httpclient的post和get方法的封装，具体不用管
 *
 *   当你查阅API，对标准业务对象进行增删改查 操作的时候， 根据文档中
 *   规定的HTTP请求方法，如果是 get ，你就调用get()方法， 请求方式是
 *   post，就调用post 方法
 *
 *
 */
@Slf4j
@Component
public class CommonRestClient {

	@Autowired
	EatonApiImpl eatonApi;

	private static final int DEFAULT_SOCKET_TIMEOUT = 120000;
	private static final int DEFAULT_CONNECTION_TIMEOUT = 60000;
	private static final String CHARSET = "utf-8";

	private static final CommonRestClient INSTANCE = new CommonRestClient();
	private static SSLConnectionSocketFactory sslConnectionSocketFactory;
	private static RequestConfig config;

	public static CommonRestClient getInstance() {
		return INSTANCE;
	}

	private CommonRestClient() {
		initTrustHosts();
		initConfig();
	}

	private void initConfig() {
		config = RequestConfig.custom().setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
				.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT).build();
	}

	private void initTrustHosts() {
		try {
			SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
				// 信任所有
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

	public CloseableHttpClient createClient()  {
		return HttpClients.custom().setDefaultRequestConfig(config).setSSLSocketFactory(sslConnectionSocketFactory)
				.build();
	}


	public String get(String accessToken, String url, Map<String, Object> params) throws IOException {
		url = url.trim();
		StringBuilder sb = new StringBuilder(url);
		String result = null;
		if (params != null && !params.isEmpty()) {
			List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
			for (Map.Entry<String, Object> entry : params.entrySet()) {
				String value = String.valueOf(entry.getValue());
				if (value != null) {
					pairs.add(new BasicNameValuePair(entry.getKey(), value));
				}
			}
			sb.append("?").append(EntityUtils.toString(new UrlEncodedFormEntity(pairs, CHARSET)));
		}
		HttpGet httpGet = new HttpGet(sb.toString());
		if (StringUtils.isNotBlank(accessToken)) {
			httpGet.addHeader("token", accessToken);
		}
		CloseableHttpResponse response = CommonRestClient.getInstance().createClient().execute(httpGet);
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				result = EntityUtils.toString(entity, CHARSET);
				EntityUtils.consume(entity);
			}
		} else {
			httpGet.abort();
		}
		response.close();
		return result;
	}

	public String bearerGet(String accessToken, String url, Map<String, Object> params) throws IOException {
		url = url.trim();
		StringBuilder sb = new StringBuilder(url);
		String result = null;
		if (params != null && !params.isEmpty()) {
			List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
			for (Map.Entry<String, Object> entry : params.entrySet()) {
				String value = String.valueOf(entry.getValue());
				if (value != null) {
					pairs.add(new BasicNameValuePair(entry.getKey(), value));
				}
			}
			sb.append("?").append(EntityUtils.toString(new UrlEncodedFormEntity(pairs, CHARSET)));
		}
		HttpGet httpGet = new HttpGet(sb.toString());
		if (StringUtils.isNotBlank(accessToken)) {
			httpGet.addHeader("Authorization", "Bearer "+accessToken);
		}
		CloseableHttpResponse response = CommonRestClient.getInstance().createClient().execute(httpGet);
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				result = EntityUtils.toString(entity, CHARSET);
				EntityUtils.consume(entity);
			}
		} else {
			httpGet.abort();
		}
		response.close();
		return result;
	}

	public JSONObject bearerGetForEaton(String accessToken, String url, Map<String, Object> params) throws IOException {
		//Map<String,Object> returnResult = new HashMap<>();
		JSONObject returnJson = new JSONObject();
		url = url.trim();
		StringBuilder sb = new StringBuilder(url);
		String result = null;
		if (params != null && !params.isEmpty()) {
			List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
			for (Map.Entry<String, Object> entry : params.entrySet()) {
				String value = String.valueOf(entry.getValue());
				if (value != null) {
					pairs.add(new BasicNameValuePair(entry.getKey(), value));
				}
			}
			sb.append("?").append(EntityUtils.toString(new UrlEncodedFormEntity(pairs, CHARSET)));
		}
		HttpGet httpGet = new HttpGet(sb.toString());
		if (StringUtils.isNotBlank(accessToken)) {
			httpGet.addHeader("Authorization", "Bearer "+accessToken);
		}
		CloseableHttpResponse response = CommonRestClient.getInstance().createClient().execute(httpGet);
		int statusCode = response.getStatusLine().getStatusCode();

		if (statusCode == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				result = EntityUtils.toString(entity, CHARSET);
				//returnResult.put("result",result);
				returnJson.put("result",result);
				Header[] headers = response.getHeaders("hasMoreRecords");
				if(headers.length>0){
					Header hasMoreRecords = headers[0];
					//returnResult.put("hasMoreRecords",hasMoreRecords.getValue());
					returnJson.put("hasMoreRecords",hasMoreRecords.getValue());
				}
				EntityUtils.consume(entity);
			}
		} else if(statusCode == 401){
			log.info("token 过期，开始重试！");
			String token = eatonApi.getToken();
			bearerGetForEaton( token,  url,  params);
		}else {
			log.info("调用出错："+statusCode+response.toString());
			httpGet.abort();
		}
		response.close();

		//return returnResult;
		return returnJson;
	}

	public String post(String accessToken, String url, String jsonObject) throws IOException {
		url = url.trim();
		StringEntity stringEntity = null;
		String result = null;
		if (jsonObject != null) {
			stringEntity = new StringEntity(jsonObject, ContentType.APPLICATION_JSON.withCharset("utf-8"));
		}

		HttpPost httpPost = new HttpPost(url);
		if (StringUtils.isNotBlank(accessToken)) {
			httpPost.addHeader("token", accessToken);
		}
		if (stringEntity != null) {
			httpPost.setEntity(stringEntity);
		}
		CloseableHttpResponse response = CommonRestClient.getInstance().createClient().execute(httpPost);
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				result = EntityUtils.toString(entity, CHARSET);
				EntityUtils.consume(entity);
			}
		} else {
			httpPost.abort();
		}
		response.close();
		return result;
	}

	public String post(String accessToken, String url, Map<String, Object> params) throws IOException {
		UrlEncodedFormEntity urlEncodedFormEntity = null;
		String result = null;
		if (params != null && !params.isEmpty()) {
			List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
			for (Map.Entry<String, Object> entry : params.entrySet()) {
				Object value = entry.getValue();
				if (value != null) {
					pairs.add(new BasicNameValuePair(entry.getKey(), String.valueOf(value)));
				}
			}
			urlEncodedFormEntity = new UrlEncodedFormEntity(pairs, CHARSET);
		}
		HttpPost httpPost = new HttpPost(url);
		if (StringUtils.isNotBlank(accessToken)) {
			httpPost.addHeader("token", accessToken);
		}
		if (urlEncodedFormEntity != null) {
			httpPost.setEntity(urlEncodedFormEntity);
		}
		CloseableHttpResponse response = CommonRestClient.getInstance().createClient().execute(httpPost);
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				result = EntityUtils.toString(entity, CHARSET);
				EntityUtils.consume(entity);
			}
		} else {
			httpPost.abort();
		}
		response.close();
		return result;
	}

	public String postToken(String accessToken, String url, Map<String, Object> params) throws IOException {
		UrlEncodedFormEntity urlEncodedFormEntity = null;
		String result = null;
		if (params != null && !params.isEmpty()) {
			List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());
			for (Map.Entry<String, Object> entry : params.entrySet()) {
				String value = String.valueOf(entry.getValue());
				if (value != null) {
					pairs.add(new BasicNameValuePair(entry.getKey(), value));
				}
			}
			urlEncodedFormEntity = new UrlEncodedFormEntity(pairs, CHARSET);
		}
		HttpPost httpPost = new HttpPost(url);
		if (StringUtils.isNotBlank(accessToken)) {
			httpPost.addHeader("token", accessToken);
		}
		if (urlEncodedFormEntity != null) {
			httpPost.setEntity(urlEncodedFormEntity);
		}
		CloseableHttpResponse response = CommonRestClient.getInstance().createClient().execute(httpPost);
		int statusCode = response.getStatusLine().getStatusCode();
		if (statusCode == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				result = EntityUtils.toString(entity, CHARSET);
				EntityUtils.consume(entity);
			}
		} else {
			httpPost.abort();
		}
		response.close();
		return result;
	}
}
