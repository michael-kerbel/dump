package util.dump.time;

/**
 * <h2>Usage example:</h2>
 *
 * <pre>
 * StopWatch interval = new StopWatch();
 * TimeUtils.sleepQuietlySeconds(62); <em>// code that needs some time</em>
 * System.out.println(interval);
 *
 * interval.reset();
 * TimeUtils.sleepQuietlySeconds(2);
 * System.out.println(interval);
 * </pre>
 *
 * <h3>Output looks like:</h3>
 * <pre>
 *  01:02.001 min
 *  2.003 s
 * </pre>
 *
 * @see TimeUtils
 */
public class StopWatch {

   private long    _start;
   private boolean _resetOnToString = false;


   public StopWatch() {
      reset();
   }

   /** returns the current interval in milliseconds */
   public long getInterval() {
      return System.currentTimeMillis() - _start;
   }

   /**
    * resets the start time to the current time
    */
   public void reset() {
      _start = System.currentTimeMillis();
   }

   /** resets the timer every time the toString() method is called.
    * @return returns this, to enable fluent usage
    */
   public StopWatch setResetOnToString( boolean resetOnToString ) {
      _resetOnToString = resetOnToString;
      return this;
   }

   @Override
   public String toString() {
      long milliseconds = System.currentTimeMillis() - _start;
      if ( _resetOnToString )
         reset();
      return TimeUtils.toHumanReadableFormat(milliseconds);
   }
}
