package crm.middleware.project.sdk.http.rest.http;

import java.util.ArrayList;
import java.util.List;

import com.rkhd.platform.sdk.http.HttpHeader;
import org.apache.commons.lang.StringUtils;

public class HttpResult
{
    private List<HttpHeader> headers;
    private String result;

    public HttpResult()
    {
        this.headers = new ArrayList();
    }

    public List<HttpHeader> getAllHeaders() {
        return this.headers;
    }

    public void setHeader(String name, String value) {
        HttpHeader httpHeader = new HttpHeader();
        httpHeader.setName(name);
        httpHeader.setValue(value);
        this.headers.add(httpHeader);
    }

    void setHeaders(List<HttpHeader> headers) {
        this.headers = headers;
    }

    public List<HttpHeader> getHeaders(String name) {
        List headerList = new ArrayList();
        if (StringUtils.isNotBlank(name)) {
            for (HttpHeader httpHeader : this.headers) {
                if (name.equals(httpHeader.getName())) {
                    headerList.add(httpHeader);
                }
            }
        }
        return headerList;
    }

    public String getResult() {
        return this.result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}