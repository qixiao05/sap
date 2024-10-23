package crm.middleware.project.cfg;


import crm.middleware.project.sdk.http.config.CrmConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Created by Shenms on 2017/3/3.
 */
@Configuration
public class WebMvcConfig extends WebMvcConfigurerAdapter {

    @Value("${xsy.CLIENT_ID}")
    private String CLIENT_ID;
    @Value("${xsy.CLIENT_SECRET}")
    private String CLIENT_SECRET;
    @Value("${xsy.USER_NAME}")
    private String USER_NAME;
    @Value("${xsy.PASSWORD}")
    private String PASSWORD;
    @Value("${xsy.SECURITY}")
    private String SECURITY;
    @Value("${xsy.REDIRECT_URI}")
    private String REDIRECT_URI;
    @Value("${xsy.API_URL}")
    private String API_URL;
    @Value("${xsy.CLIENT_NAME}")
    private String CLIENT_NAME;



    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        //registry.addResourceHandler(staticAccessPath).addResourceLocations("file:" + uploadFolder);

        //设置XsyConfig动态加载配置文件
        
        CrmConfig.CLIENT_NAME = CLIENT_NAME.toString().trim();
        CrmConfig.CLIENT_ID = CLIENT_ID.toString().trim();
        CrmConfig.CLIENT_SECRET = CLIENT_SECRET.toString().trim();
        CrmConfig.USER_NAME = USER_NAME.toString().trim();
        CrmConfig.PASSWORD = PASSWORD.toString().trim();
        CrmConfig.SECURITY = SECURITY.toString().trim();
        CrmConfig.REDIRECT_URI = REDIRECT_URI.toString().trim();
        CrmConfig.API_URL = API_URL.toString().trim();



        registry.addResourceHandler("doc.html")
                .addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("service-worker.js")
                .addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("/webjars/**")
                .addResourceLocations("classpath:/META-INF/resources/webjars/");
    }

}
