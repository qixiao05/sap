package crm.middleware.project.annotation;



import crm.middleware.project.enumm.TransEnum;

import java.lang.annotation.*;

/**
 * @program: crm.nuoya.project
 * @description:
 * @author: Mike
 * @create: 2021-02-02 17:07
 **/

@Target(value = {ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TransField {

    String oaFidld(); //oa字段 必填

    TransEnum tyep(); //类型  必填

    String describe();// 字段描述

    String converApi() default "";//转换字段API
}
