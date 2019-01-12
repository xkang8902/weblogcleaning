package cn.com.comleader.mr.weblog.preprocess;/**
 * Created by arc on 15/10/2018.
 */

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;

/**
 * This is Description
 *
 * @author arc
 * @date 2018/10/15
 */
public class WeblogParser {

    static SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static WeblogBean parser(String line) {
        WeblogBean weblogBean = new WeblogBean();
        String[] arr = line.split(" ");
        //判断什么样的数据才是需要的数据
        if (arr.length > 11) {
            weblogBean.setRemote_addr(arr[0]);
            weblogBean.setRemote_user(arr[1]);
            String time_lcoal = formatDate(arr[3].substring(1));
            if (null == time_lcoal || "".equals(time_lcoal)) time_lcoal = "-invalid_time-";
            weblogBean.setTime_local(time_lcoal);
            weblogBean.setRequest(arr[6]);
            weblogBean.setStatus(arr[8]);
            weblogBean.setBody_bytes_sent(arr[9]);
            weblogBean.setHttp_referer(arr[10]);
            if (arr.length > 12) {
                StringBuilder sb = new StringBuilder();
                for (int i = 11; i < arr.length; i++) {
                    sb.append(arr[i]);
                }
                weblogBean.setHttp_user_agent(sb.toString());
            } else {
                weblogBean.setHttp_user_agent(arr[11]);
            }
            //valid 提前设置了默认值
            if (Integer.parseInt(weblogBean.getStatus()) >= 400) weblogBean.setValid(false);
            if ("-invalid_time-".equals(weblogBean.getTime_local())) weblogBean.setValid(false);
        } else {
            weblogBean = null;
        }
        return weblogBean;
    }

    //格式化时间
    private static String formatDate(String string) {
        try {
            return sdf2.format(sdf1.parse(string));
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 过滤的方式:1.数据不需要了weblogBean=null    2.设置valid字段为false
     */
    public static void filterStaticResource(WeblogBean weblogBean, Set<String> pages) {
        if (!pages.contains(weblogBean.getRequest())) {
            weblogBean.setValid(false);
        }
    }
}
