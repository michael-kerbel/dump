package util.dump;

import util.time.StopWatch;


public class ThreadLocalBenchmark {

   public static void main( String[] args ) {
      benchmark();
      benchmark();
      benchmark();
      benchmark();
      benchmark();
      benchmark();
      benchmark();
      benchmark();
      benchmark();
      benchmark();
   }

   private static void benchmark() {
      ThreadLocal<Long> l = new ThreadLocal<Long>();

      StopWatch t = new StopWatch();
      for ( int i = 0; i < 1000000; i++ ) {
         l.set((long)i);
         l.get();
      }
      System.err.println(t);
   }
}
