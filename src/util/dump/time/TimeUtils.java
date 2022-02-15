package util.dump.time;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;


class TimeUtils {

   public static final long             SECOND_IN_MILLIS   = 1000L;
   public static final long             MINUTE_IN_MILLIS   = 60 * SECOND_IN_MILLIS;
   public static final long             HOUR_IN_MILLIS     = 60 * MINUTE_IN_MILLIS;
   public static final long             DAY_IN_MILLIS      = 24 * HOUR_IN_MILLIS;

   private static final DecimalFormatSymbols  symbols            = new DecimalFormatSymbols(Locale.ENGLISH);
   private static final DecimalFormat         dual               = new DecimalFormat("00", symbols);
   private static final DecimalFormat         sf                 = new DecimalFormat("00.0##", symbols);

   /**
    * writes an interval in a nice human readable format.
    *
    * Example:
    * <pre>
    * 478871 becomes "07:58.871 min"
    * </pre>
    */
   public static String toHumanReadableFormat( long milliseconds ) {

      StringBuilder sb = new StringBuilder(16);

      if ( milliseconds < 0 ) {
         sb.append("-");
         milliseconds = -milliseconds;
      }

      // days
      if ( milliseconds >= DAY_IN_MILLIS ) {
         long days = milliseconds / DAY_IN_MILLIS;
         milliseconds -= days * DAY_IN_MILLIS;
         sb.append(days);
         if ( days == 1 ) {
            sb.append(" day ");
         } else {
            sb.append(" days ");
         }
      }

      // minutes
      if ( milliseconds >= MINUTE_IN_MILLIS ) {
         long hours = milliseconds / HOUR_IN_MILLIS;
         milliseconds -= hours * HOUR_IN_MILLIS;
         long minutes = milliseconds / MINUTE_IN_MILLIS;
         long seconds = milliseconds - minutes * MINUTE_IN_MILLIS;
         if ( hours > 0 ) {
            sb.append(dual.format(hours)).append(":");
         }

         sb.append(dual.format(minutes)).append(":");
         sb.append(sf.format(seconds / 1000.0));

         if ( hours > 0 ) {
            sb.append(" h");
         } else {
            sb.append(" min");
         }
      }

      // seconds
      else if ( Math.abs(milliseconds) >= 1000 ) {
         sb.append(milliseconds / 1000.0).append(" s");
      }
      // ms
      else {
         sb.append(milliseconds).append(" ms");
      }

      return sb.toString();
   }

}
