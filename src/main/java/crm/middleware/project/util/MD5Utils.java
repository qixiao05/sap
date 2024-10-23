package crm.middleware.project.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {

    public static String encrypt(String s) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(s.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md5.digest();
            StringBuilder build = new StringBuilder();

            for (int i = 0; i < digest.length; ++i) {
                //以16进制形式输出,高位不足补0
                build.append(String.format("%02X", digest[i]));
            }

            return build.toString();
        } catch (NoSuchAlgorithmException var5) {
            var5.printStackTrace();
            throw new RuntimeException(var5);
        }
    }

}
