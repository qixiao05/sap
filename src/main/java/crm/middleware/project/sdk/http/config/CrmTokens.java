package crm.middleware.project.sdk.http.config;

import java.util.HashMap;
import java.util.Map;

public class CrmTokens {

    private static final Map<String, String> tokenMap = new HashMap<String, String>();

    public static String getToken(String identifyStr) {
        String token = tokenMap.get(identifyStr);
        return token;
    }

    public static void setToken(String identifyStr, String token) {
        tokenMap.put(identifyStr, token);
    }

    public static void removeToken(String key) {
        tokenMap.remove(key);
    }
}
