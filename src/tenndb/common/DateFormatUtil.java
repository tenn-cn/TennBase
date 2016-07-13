package tenndb.common;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

public class DateFormatUtil {
	
	private static final String GMT_TIME_ZONE = "GMT+08:00".intern();
	
    public static ThreadLocal<SimpleDateFormat> threadLocalDateFormat(final String pattern) 
    {
        ThreadLocal<SimpleDateFormat> tl = new ThreadLocal<SimpleDateFormat>() {
            protected SimpleDateFormat initialValue() {
                SimpleDateFormat df = new SimpleDateFormat(pattern, Locale.ENGLISH);
                df.setTimeZone(TimeZone.getTimeZone(DateFormatUtil.GMT_TIME_ZONE));
                return df;
            }
        };
        return tl;
    }
    
    public static ThreadLocal<SimpleDateFormat> threadLocalDateFormat
    (final String pattern, final TimeZone time_zone) 
    {
        ThreadLocal<SimpleDateFormat> tl = new ThreadLocal<SimpleDateFormat>() 
        {
            protected SimpleDateFormat initialValue() {
                SimpleDateFormat df = new SimpleDateFormat(pattern, Locale.ENGLISH);
                df.setTimeZone(time_zone);
                return df;
            }
        };
        return tl;
    }
}
