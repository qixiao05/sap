package crm.middleware.project.cfg;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Shenms on 2017/3/3.
 */
@Configuration
public class CrmPropertiesConfig {


    @Value("${eaton.user}")
    public String user;

    @Value("${eaton.pwd}")
    public String pwd;

    @Value("${eaton.tokenUrl}")
    public String tokenUrl;

    @Value("${eaton.salesorders}")
    public String salesorders;

    @Value("${eaton.delivery}")
    public String delivery;

    @Value("${eaton.billing}")
    public String billing;

    @Value("${order.entityType}")
    public String orderEntityType;

    @Value("${orderDetail.entityType}")
    public String orderDetailEntityType;

    @Value("${billing.entityType}")
    public String billingEntityType;

    @Value("${billingDetail.entityType}")
    public String billingDetailEntityType;

    @Value("${delivery.entityType}")
    public String deliveryEntityType;

    @Value("${deliveryDetail.entityType}")
    public String deliveryDetailEntityType;

    @Value("${oppo.OEMEntityType}")
    public String oppoOEMEntityType;

    @Value("${predicted.defaultEntityType}")
    public String predictedDefaultEntityType;

    @Value("${middlewareLog.entityType}")
    public String middlewareLogEntityType;




}
