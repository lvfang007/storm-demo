package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: LvFang
 * @Date: Created in 2018/9/13.
 * @Description:
 */
public class DemoUtils {

    public static final int TIME_COUNT = 1000 * 60 * 60 * 24 * 3;

    public static void waitForSeconds(int seconds){
        try {
            Thread.sleep(seconds*1000);
        } catch (Exception e) {
        }
    }

    public static void waitForMillis(long milliseconds){
        try {
            Thread.sleep(milliseconds);
        } catch (Exception e) {}
    }

    public static String getTimeStr() {
        String timeFalg = "";
        try {
            Long time = new Date().getTime() - TIME_COUNT;
//            Long signTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2018-11-01 00:00:00").getTime();
            timeFalg = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
        }catch (Exception e){
            e.printStackTrace();
        }
        return timeFalg;
    }
}

