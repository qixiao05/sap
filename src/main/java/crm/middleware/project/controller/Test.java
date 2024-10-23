package crm.middleware.project.controller;

import com.alibaba.fastjson.JSONObject;
import crm.middleware.project.sdk.CrmAPIs;
import crm.middleware.project.sdk.http.rest.RestClient;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

public class Test {
    @Autowired
    CrmAPIs crmAPIs;
    public void test() throws Exception{
        JSONObject bodyData = new JSONObject();
        Map<String,String> formData = new HashMap<>();
        RestClient.commonHttpClient("https://api-p10.xiaoshouyi.com/rest/data/v2.0/scripts/api/xsy/createLead", formData, null, "POST", null);

    }
}
