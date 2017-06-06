package util.dump;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import junit.framework.Assert;
import util.concurrent.ExecutorUtils;
import util.time.TimeUtils;


public class MultithreadedDumpTest {

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(new FileFilter() {

         @Override
         public boolean accept( File f ) {
            return f.getName().startsWith("DumpTest.");
         }
      });
      for ( File df : dumpFile ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Test
   public void testMultipleThreads() throws IOException, NoSuchFieldException {
      File dumpFile = new File("DumpTest.dmp");
      MultithreadedDump<Bean> dump = new MultithreadedDump<Bean>(Bean.class, dumpFile);
      UniqueIndex<Bean> index = new UniqueIndex<Bean>(dump, "_id");
      ThreadPoolExecutor executor = ExecutorUtils.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), "worker");
      for ( int j = 0; j < 100000; j++ ) {
         Bean bean = new Bean(j);
         executor.execute(() -> dump.addSilently(bean));
         while ( executor.getQueue().size() > 10000 ) {
            TimeUtils.sleepQuietly(1);
         }
      }
      ExecutorUtils.awaitCompletion(executor, 1, 10000);

      Bean bean = index.lookup(50000);
      Assert.assertNotNull("Bean added after reopening not found using index", bean);
      Assert.assertEquals("wrong index for bean retrieved using index", 50000, bean._id);

      TIntSet ids = new TIntHashSet();
      for ( Bean b : dump ) {
         ids.add(b._id);
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, ids.size());

      dump.close();
   }

   @Test
   public void testReOpening() throws IOException, NoSuchFieldException {
      File dumpFile = new File("DumpTest.dmp");
      Dump<Bean> dump = new MultithreadedDump<Bean>(Bean.class, dumpFile);
      UniqueIndex<Bean> index = new UniqueIndex<Bean>(dump, "_id");
      for ( int j = 0; j < 100000; j++ ) {
         dump.add(new Bean(j));
      }
      Bean bean = index.lookup(50000);
      Assert.assertNotNull("Bean added after reopening not found using index", bean);
      Assert.assertEquals("wrong index for bean retrieved using index", 50000, bean._id);

      int i = 0;
      for ( Bean b : dump ) {
         Assert.assertEquals("Wrong index during iteration", i++, b._id);
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, i);

      dump.close();

      dump = new MultithreadedDump<Bean>(Bean.class, dumpFile);
      index = new UniqueIndex<Bean>(dump, "_id");
      for ( int j = 100000; j < 200000; j++ ) {
         dump.add(new Bean(j));
      }

      bean = index.lookup(150000);
      Assert.assertNotNull("Bean added after reopening not found using index", bean);
      Assert.assertEquals("wrong index for bean retrieved using index", 150000, bean._id);

      i = 0;
      for ( Bean b : dump ) {
         if ( i % 2 == 0 ) {
            dump.deleteLast();
         }
         Assert.assertEquals("Wrong index during iteration", i++, b._id);
      }
      Assert.assertEquals("Iterated wrong number of elements", 200000, i);

      /* test deletions */
      i = 0;
      for ( Bean b : dump ) {
         Assert.assertEquals("Wrong index during iteration", i * 2 + 1, b._id);
         i++;
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, i);

      dump.close();

      dump = new MultithreadedDump<Bean>(Bean.class, dumpFile);

      /* test deletions after re-opening*/
      i = 0;
      for ( Bean b : dump ) {
         Assert.assertEquals("Wrong index during iteration", i * 2 + 1, b._id);
         i++;
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, i);

      dump.close();
   }


   public static class Bean implements ExternalizableBean {

      @externalize(1)
      int _id;


      public Bean() {}

      public Bean( int id ) {
         _id = id;
      }
   }
}
