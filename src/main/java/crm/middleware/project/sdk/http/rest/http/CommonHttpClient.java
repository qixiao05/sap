//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package crm.middleware.project.sdk.http.rest.http;

import com.rkhd.platform.sdk.exception.XsyHttpException;
import com.rkhd.platform.sdk.http.CommonData;
import com.rkhd.platform.sdk.http.CommonResponse;
import com.rkhd.platform.sdk.http.HttpDeleteWithEntity;
import com.rkhd.platform.sdk.http.HttpHeader;
import com.rkhd.platform.sdk.http.handler.ResponseBodyHandler;
import com.rkhd.platform.sdk.log.Logger;
import com.rkhd.platform.sdk.log.LoggerFactory;
import com.rkhd.platform.sdk.util.IOUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class CommonHttpClient {
    private CloseableHttpClient client;
    private static final Logger logger = LoggerFactory.getLogger();
    private String contentEncoding = "UTF-8";
    private String contentType = "application/json";
    private int socketTimeout = 30000;
    private int connectionTimeout = 30000;
    private RequestConfig config;

    /** @deprecated */
    @Deprecated
    public CommonHttpClient() {
        this.createClientWithoutSSL();
    }

    public static CommonHttpClient instance() {
        return new CommonHttpClient();
    }

    /** @deprecated */
    @Deprecated
    public String performRequest(CommonData data) {
        HttpResult httpResult = this.execute(data);
        return httpResult != null ? httpResult.getResult() : null;
    }

    public HttpResult execute(CommonData data) {
        HttpResult httpResult = new HttpResult();

        try {
            HttpResponse response = this.executeRequest(data);
            if (response != null) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    httpResult.setResult(EntityUtils.toString(entity, StandardCharsets.UTF_8.displayName()));
                }

                httpResult.setHeaders(this.getHeaders(response.getAllHeaders()));
            }
        } catch (Exception var5) {
            logger.error("Problem performing request: " + var5.getMessage(), var5);
            httpResult.setResult(var5.getMessage());
        }

        return httpResult;
    }

    private List<HttpHeader> getHeaders(Header[] headers) {
        List<HttpHeader> httpHeaders = new ArrayList();
        if (headers != null && headers.length > 0) {
            Header[] var3 = headers;
            int var4 = headers.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                Header header = var3[var5];
                HttpHeader httpHeader = new HttpHeader();
                httpHeader.setName(header.getName());
                httpHeader.setValue(header.getValue());
                httpHeaders.add(httpHeader);
            }
        }

        return httpHeaders;
    }

    public <T> CommonResponse<T> execute(CommonData commonData, ResponseBodyHandler<T> handler) throws XsyHttpException {
        try {
            HttpResponse httpResponse = this.executeRequest(commonData);
            CommonResponse<T> response = new CommonResponse();
            if (httpResponse != null) {
                response.setCode(httpResponse.getStatusLine().getStatusCode());
                response.setHeaders(this.getHeaders(httpResponse.getAllHeaders()));
                HttpEntity entity = httpResponse.getEntity();
                String data = null;
                if (entity != null) {
                    data = EntityUtils.toString(entity, StandardCharsets.UTF_8.displayName());
                }

                response.setData(handler.handle(data));
            }

            return response;
        } catch (Exception var7) {
            logger.error(var7.getMessage(), var7);
            throw new XsyHttpException(var7.getMessage(), 100000L, var7);
        }
    }

    public RkhdFile downFile(CommonData data) throws XsyHttpException {
        try {
            HttpResponse httpResponse = this.executeRequest(data);
            InputStream is = httpResponse.getEntity().getContent();
            String fileContent = IOUtil.toStringWithLimit(is);
            Header fileNameHeader = httpResponse.getFirstHeader("Content-Disposition");
            String fileName = null;
            if (fileNameHeader != null && fileNameHeader.getValue() != null) {
                fileName = fileNameHeader.getValue().replaceFirst("(?i)^.*filename=\"?([^\"]+)\"?.*$", "$1");
            }

            return new RkhdFile(fileName, fileContent);
        } catch (Exception var7) {
            logger.error(var7.getMessage(), var7);
            throw new XsyHttpException(var7.getMessage(), 100000L, var7);
        }
    }

    private HttpResponse executeRequest(CommonData data) throws IOException {
        HttpResponse response = null;
        String var3 = ((CommonData)Optional.of(data).get()).getCall_type().toUpperCase();
        byte var4 = -1;
        switch(var3.hashCode()) {
            case 70454:
                if (var3.equals("GET")) {
                    var4 = 0;
                }
                break;
            case 79599:
                if (var3.equals("PUT")) {
                    var4 = 3;
                }
                break;
            case 2461856:
                if (var3.equals("POST")) {
                    var4 = 1;
                }
                break;
            case 75900968:
                if (var3.equals("PATCH")) {
                    var4 = 2;
                }
                break;
            case 2012838315:
                if (var3.equals("DELETE")) {
                    var4 = 4;
                }
        }

        String urlStr;
        switch(var4) {
            case 0:
                urlStr = data.getCallString();
                HttpGet get = new HttpGet(urlStr);
                Iterator var7 = data.getHeaderList().iterator();

                while(var7.hasNext()) {
                    HttpHeader httpHeader = (HttpHeader)var7.next();
                    if ("Authorization".equals(httpHeader.getName())) {
                        get.setHeader(httpHeader.getName(), httpHeader.getValue());
                    } else {
                        get.addHeader(httpHeader.getName(), httpHeader.getValue());
                    }
                }

                response = this.client.execute(get);
                break;
            case 1:
                urlStr = data.getCallString();
                HttpPost post = new HttpPost(urlStr);
                response = this.executeHttpEntityEnclosingRequestBase(post, data);
                break;
            case 2:
                urlStr = data.getCallString();
                HttpPatch patch = new HttpPatch(urlStr);
                response = this.executeHttpEntityEnclosingRequestBase(patch, data);
                break;
            case 3:
                urlStr = data.getCallString();
                HttpPut put = new HttpPut(urlStr);
                response = this.executeHttpEntityEnclosingRequestBase(put, data);
                break;
            case 4:
                urlStr = data.getCallString();
                HttpDeleteWithEntity delete = new HttpDeleteWithEntity(urlStr);
                response = this.executeHttpEntityEnclosingRequestBase(delete, data);
                break;
            default:
                urlStr = "Unknown call type: [" + data.getCall_type() + "]";
                logger.error(urlStr);
                throw new IOException(urlStr);
        }

        return (HttpResponse)response;
    }

    private HttpResponse executeHttpEntityEnclosingRequestBase(HttpEntityEnclosingRequestBase request, CommonData data) throws IOException {
        Iterator var3 = data.getHeaders().entrySet().iterator();

        while(var3.hasNext()) {
            Entry<String, String> entry = (Entry)var3.next();
            request.addHeader((String)entry.getKey(), (String)entry.getValue());
        }

        if (data.getFormData().size() != 0) {
            if ("urlEncoded".equals(data.getFormType())) {
                UrlEncodedFormEntity postEntity = new UrlEncodedFormEntity(this.getParam(data.getFormData()), this.contentEncoding);
                request.setEntity(postEntity);
            } else {
                MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                Iterator var10 = data.getFormData().entrySet().iterator();

                while(var10.hasNext()) {
                    Entry<String, Object> entry = (Entry)var10.next();
                    if (entry.getValue() instanceof RkhdFile) {
                        RkhdFile file = (RkhdFile)entry.getValue();
                        if (file.getFileName() == null) {
                            throw new IOException("RkhdFile name can not be null");
                        }

                        builder.addBinaryBody((String)entry.getKey(), file.getFileContent().getBytes("UTF-8"), ContentType.create("multipart/form-data"), file.getFileName());
                    } else {
                        builder.addTextBody((String)entry.getKey(), entry.getValue().toString());
                    }
                }

                request.setEntity(builder.build());
            }
        } else {
            StringEntity se = new StringEntity(data.getBody(), this.contentEncoding);
            se.setContentType(this.contentType);
            se.setContentEncoding(new BasicHeader("Content-Encoding", this.contentEncoding));
            request.setEntity(se);
        }

        data.setCallString(request.getURI().toString());
        return this.client.execute(request);
    }

    private List<NameValuePair> getParam(Map parameterMap) {
        List<NameValuePair> param = new ArrayList();

        Entry parmEntry;
        Object value;
        for(Iterator it = parameterMap.entrySet().iterator(); it.hasNext(); param.add(new BasicNameValuePair((String)parmEntry.getKey(), value.toString()))) {
            parmEntry = (Entry)it.next();
            value = parmEntry.getValue();
            if (value == null) {
                value = "";
            }
        }

        return param;
    }

    public void close() {
        try {
            this.client.close();
        } catch (IOException var2) {
            var2.printStackTrace();
        }

    }

    public void createClientWithoutSSL() {
        try {
            this.config = RequestConfig.custom().setConnectTimeout(this.connectionTimeout).setSocketTimeout(this.socketTimeout).build();
            SSLContext sslContext = (new SSLContextBuilder()).loadTrustMaterial((KeyStore)null, new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            this.client = HttpClients.custom().setSSLSocketFactory(sslsf).setDefaultRequestConfig(this.config).build();
        } catch (KeyManagementException var3) {
            var3.printStackTrace();
        } catch (NoSuchAlgorithmException var4) {
            var4.printStackTrace();
        } catch (KeyStoreException var5) {
            var5.printStackTrace();
        }

    }

    public void createSSLClient() {
        this.config = RequestConfig.custom().setConnectTimeout(this.connectionTimeout).setSocketTimeout(this.socketTimeout).build();
        this.client = HttpClients.custom().setDefaultRequestConfig(this.config).build();
    }

    public String getContentEncoding() {
        return this.contentEncoding;
    }

    public void setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    public String getContentType() {
        return this.contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
}
