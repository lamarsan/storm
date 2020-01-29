package com.lamarsan.bigdata.utils;

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * className: DateUtils
 * description: TODO
 *
 * @author lamar
 * @version 1.0
 * @date 2020/1/27 0:08
 */
public class DateUtils {
    private DateUtils(){}

    private static DateUtils instance;

    public static DateUtils getInstance(){
        if (instance == null) {
            instance = new DateUtils();
        }

        return instance;
    }

    private FastDateFormat format = FastDateFormat.getInstance("yyyyMMddHHmmss");

    public long getTime(String time) throws Exception {
        return format.parse(time).getTime();
    }
}
