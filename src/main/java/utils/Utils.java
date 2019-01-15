package utils;

/**
 * @Author: LvFang
 * @Date: Created in 2018/9/13.
 * @Description:
 */
public class Utils {

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
}

