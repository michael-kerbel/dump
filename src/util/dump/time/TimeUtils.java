package util.dump.time;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;


public class TimeUtils {

   public static final long             SECOND_IN_MILLIS   = 1000L;
   public static final long             MINUTE_IN_MILLIS   = 60 * SECOND_IN_MILLIS;
   public static final long             HOUR_IN_MILLIS     = 60 * MINUTE_IN_MILLIS;
   public static final long             DAY_IN_MILLIS      = 24 * HOUR_IN_MILLIS;
   public static final long             WEEKS_IN_MILLIS    = 7 * DAY_IN_MILLIS;
   public static final long             MONTHS_IN_MILLIS   = 30 * DAY_IN_MILLIS;
   public static final long             QUARTERS_IN_MILLIS = 91 * DAY_IN_MILLIS;
   public static final long             YEARS_IN_MILLIS    = 365 * DAY_IN_MILLIS;

   private static DecimalFormatSymbols  symbols            = new DecimalFormatSymbols(Locale.ENGLISH);
   private static DecimalFormat         dual               = new DecimalFormat("00", symbols);
   private static DecimalFormat         sf                 = new DecimalFormat("00.0##", symbols);

   private static ThreadLocal<Calendar> CALENDAR           = new ThreadLocal<Calendar>() {

                                                              @Override
                                                              protected Calendar initialValue() {
                                                                 return Calendar.getInstance();
                                                              }
                                                           };


   /**
    * checks if a Date is older than the number of days
    *
    * Example:
    * <pre>
    * Date oneWeekAgo = …
    * dateOlderThanDays(oneWeekAgo, 3)  → true
    * dateOlderThanDays(oneWeekAgo, 8)  → false
    * </pre>
    */
   public static boolean dateOlderThanDays( Date date, long days ) {
      long now = System.currentTimeMillis();
      if ( date.getTime() < now - days * DAY_IN_MILLIS ) {
         return true;
      } else {
         return false;
      }
   }

   public static int getCalendarFieldValue( Date date, int calendarField ) {
      Calendar calendar = CALENDAR.get();
      calendar.setTime(date);
      return calendar.get(calendarField);
   }

   /**
    * <p>Returns the date that is rounded to a daily basis<p>
    *
    * Example:<br/>
    *   roundDateToFullDay for the Date "2009-04-22 17:43:13" would return "2009-04-22 00:00:00"
    */
   public static Date roundDateToFullDay( final Date originalDate ) {
      Calendar calendar = CALENDAR.get();
      calendar.setTime(roundDateToFullHour(originalDate));
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      return calendar.getTime();
   }

   /**
    * <p>Returns the date that is rounded to a daily basis<p>
    *
    * Example:<br/>
    *   roundDateToFullDay for the Date "2009-04-22 17:43:13" would return "2009-04-22 17:00:00"
    */
   public static Date roundDateToFullHour( final Date originalDate ) {
      Calendar calendar = CALENDAR.get();
      calendar.setTime(originalDate);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      return calendar.getTime();
   }

   /**
    * <p>Returns the date that is rounded to a monthly basis<p>
    *
    * Example:<br/>
    *   roundDateToFullMonth for the Date "2009-04-22 17:43:13" would return "2009-04-01 00:00:00"
    */
   public static Date roundDateToFullMonth( final Date originalDate ) {
      Calendar calendar = CALENDAR.get();
      calendar.setTime(roundDateToFullDay(originalDate));
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      return calendar.getTime();
   }

   /**
    * <p>Returns the date that is rounded to a weekly basis. Beginning of the week is <b>Monday</b>.<p>
    *
    * Example:<br/>
    *   roundDateToFullWeek for the Date "2009-04-22 17:43:13" would return "2009-04-20 00:00:00"
    */
   public static Date roundDateToFullWeek( final Date originalDate ) {
      Calendar calendar = CALENDAR.get();
      calendar.setTime(roundDateToFullDay(originalDate));
      calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
      return calendar.getTime();
   }

   /**
    * <p>calls Thread.sleep but immediately returns on an {@link InterruptedException} though it makes sure the interrupted status is saved</p>
    * see <a href="http://www.javaspecialists.eu/archive/Issue146.html">The Law of the Sabotaged Doorbell</a>
    *
    * <p><b>Note:</b> There's no guarantee of how long the thread will actually sleep.
    * Tests even showed, that sleepQuietly(100) might sleep more than <b>300 ms</b> if there's some load on the system!</p>
    *
    * @param milliseconds   time to sleep in milliseconds
    */
   public static void sleepQuietly( long milliseconds ) {
      try {
         TimeUnit.MILLISECONDS.sleep(milliseconds);
      }
      /**
       * <quote>
       * Note that the interrupted status is set to false when an InterruptedException is caused
       * or when the Thread.interrupt method is explicitly called.
       * Thus, when we catch an InterruptedException, we need to remember that the thread is now not interrupted anymore!
       * In order to have orderly shutdown of the thread, we should keep the thread set to "interrupted".
       * </quote>
       */
      catch ( InterruptedException e ) {
         Thread.currentThread().interrupt();
      }
   }

   /**
    * @see #sleepQuietly(long)
    * @param seconds    time to sleep in seconds
    */
   public static void sleepQuietlySeconds( long seconds ) {
      sleepQuietly(seconds * SECOND_IN_MILLIS);
   }

   /**
    * writes an interval in a nice human readable format.
    *
    * Example:
    * <pre>
    * 478871 becomes "07:58.871 min"
    * </pre>
    */
   public static String toHumanReadableFormat( long milliseconds ) {

      StringBuffer sb = new StringBuffer(16);

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

   /**
    * @see #toHumanReadableFormat(long)
    */
   public static String toHumanReadableFormatNano( long nanoseconds ) {
      return toHumanReadableFormat(TimeUnit.NANOSECONDS.toMillis(nanoseconds));
   }
}
