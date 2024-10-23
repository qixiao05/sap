package crm.middleware.project.sdk.http.config;


/**
 * @Description:
 * @Auther: Shenms
 * @Date: 2019.06.17 11:06
 * @Version: 1.0
 */
public class CrmApiUrl {

    public static String url_v1_oauth = CrmConfig.API_URL + "/oauth2/token.action";

    public static String url_v1_query = CrmConfig.API_URL + "/data/v1/query";

    public static String url_v1 = CrmConfig.API_URL + "/data/v1/objects";

    public static String url_notify_create = CrmConfig.API_URL + "/data/v1/notice/notify/send";

    public static String url_user_list = CrmConfig.API_URL + "/data/v1/objects/user/list";

    public static String url_v2_query_xoql = CrmConfig.API_URL + "/rest/data/v2.0/query/xoql";

    public static String url_v2 = CrmConfig.API_URL + "/rest/data/v2/objects";

    public static String url_v2_xobjects = CrmConfig.API_URL + "/rest/data/v2.0/xobjects";

    public static String url_v2_query = CrmConfig.API_URL + "/rest/data/v2/query";

    public static String url_v2_notice = CrmConfig.API_URL + "/rest/notice/v2.0/newNotice";

    public static String url_v2_bulk = CrmConfig.API_URL + "/rest/bulk/v2";



}
