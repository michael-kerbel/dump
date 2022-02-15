package util.dump;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.dump.time.TimeUtils;

import javax.annotation.Nonnull;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class ExecutorUtils {

   private static Logger _log = LoggerFactory.getLogger(ExecutorUtils.class);


   /**
    * Waits for all scheduled tasks of an Executor to finish, and shuts it down.<br/>
    * Do not use this, when adding tasks to the Executor after calling this method!<br/><br/>
    * Uses 1000 ms as polling interval to check if finished and a max time of 30000 ms 
    * for awaiting termination of the Executor after all tasks have been completed.
    */
   public static void awaitCompletion( @Nonnull ThreadPoolExecutor executor ) {
      awaitCompletion(executor, 1000, 30000);
   }

   /**
    * Waits for all scheduled tasks of an Executor to finish, and shuts it down.<br/>
    * Do not use this, when adding tasks to the Executor after calling this method!  
    * @param sleepIntervalInMillis The polling interval for the checks on the Executor
    * @param shutdownWaitIntervalInMillis The maximum time to wait for the shutdown of the Executor after all tasks have been completed
    */
   public static void awaitCompletion( @Nonnull ThreadPoolExecutor executor, long sleepIntervalInMillis, long shutdownWaitIntervalInMillis ) {
      long taskCount = getTaskCount(executor);
      while ( true ) {
         boolean check = false;
         synchronized ( executor ) {
            if ( executor.isShutdown() ) {
               throw new RuntimeException("Task executor was stopped early.");
            }
            check = executor.getQueue().size() > 0;
            check |= executor.getActiveCount() > 0;
         }
         if ( !check ) {
            break;
         }
         TimeUtils.sleepQuietly(sleepIntervalInMillis);
      }

      synchronized ( executor ) {
         executor.shutdown();
         try {
            executor.awaitTermination(shutdownWaitIntervalInMillis, TimeUnit.MILLISECONDS);
         }
         catch ( InterruptedException argh ) {
            // ignore
         }
      }

      long completedTaskCount = executor.getCompletedTaskCount();
      if ( taskCount > completedTaskCount ) {
         _log.warn("executed too few tasks! At least " + taskCount + " were scheduled, but only " + completedTaskCount + " were executed.");
      }
   }

   public static ThreadPoolExecutor newFixedThreadPool( int nThreads, String threadNamePrefix ) {
      return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
         new NamedThreadFactory(threadNamePrefix));
   }

   private static long getTaskCount( ThreadPoolExecutor executor ) {
      TIntIntMap counts = new TIntIntHashMap();
      for ( int i = 5; i >= 0; i-- ) {
         counts.adjustOrPutValue((int)executor.getTaskCount(), 1, 1);
      }
      int maxCount = 0, taskCount = 0;
      for ( int tc : counts.keys() ) {
         int c = counts.get(tc);
         if ( c > maxCount ) {
            maxCount = c;
            taskCount = tc;
         }
      }
      return taskCount;
   }


   /** this is a copy&paste of the DefaultThreadFactory with the namePrefix made customizable */
   public static class NamedThreadFactory implements ThreadFactory {

      private final ThreadGroup   group;
      private final AtomicInteger threadNumber = new AtomicInteger(1);
      private final String        namePrefix;


      public NamedThreadFactory( String prefix ) {
         SecurityManager s = System.getSecurityManager();
         group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
         namePrefix = prefix + "-thread-";
      }

      @Override
      public Thread newThread( Runnable r ) {
         Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
         if ( t.isDaemon() ) {
            t.setDaemon(false);
         }
         if ( t.getPriority() != Thread.NORM_PRIORITY ) {
            t.setPriority(Thread.NORM_PRIORITY);
         }
         return t;
      }
   }
}
