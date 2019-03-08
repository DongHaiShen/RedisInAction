package chap05;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * @Author sdh
 * @Date Created in 2019/3/8 10:37
 * @description
 */
public class Constant
{
    // 日志安全级别
    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARNING = "warning";
    public static final String ERROR = "error";
    public static final String CRITICAL = "critical";

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");
    public static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    static
    {
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
}
