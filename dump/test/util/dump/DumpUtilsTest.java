package util.dump;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.assertj.core.util.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import util.dump.DumpTest.Bean;


public class DumpUtilsTest {

   private static final int NUMBER_OF_INSTANCES = 10000;

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(new FileFilter() {

         @Override
         public boolean accept( File f ) {
            return f.getName().startsWith("DumpUtilsTest");
         }
      });
      for ( File df : dumpFile ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Test
   public void testCleanup_broken() throws Exception {
      testCleanupBroken(200, "broken data", 0);
   }

   @Test
   public void testCleanup_brokenAndDeleted() throws Exception {
      testCleanupBroken(200, "broken data", 3);
   }

   @Test
   public void testCleanup_brokenMoreAndDeleted() throws Exception {
      testCleanupBroken(200, "broken data broken data broken data", 3);
   }

   @Test
   public void testCleanup_deleted() throws IOException {
      Dump<Bean> source = null;
      Dump<Bean> dest = null;
      try {
         source = createSourceDump();

         long sourceSizeAfterAdding = source.getDumpSize();

         deleteFromDump(source, 2);

         long sourceSizeAfterDeleting = source.getDumpSize();

         Assert.assertTrue("Dump shrank after deletions", sourceSizeAfterAdding <= sourceSizeAfterDeleting);

         dest = createDestDump();

         DumpUtils.cleanup(source, dest);

         long destSizeAfterCleanup = dest.getDumpSize();

         Assert.assertTrue("Dump did not shrink after cleanup", destSizeAfterCleanup < sourceSizeAfterDeleting);
      }
      finally {
         DumpUtils.closeSilently(source);
         DumpUtils.closeSilently(dest);
      }
   }

   @Test
   public void testDeleteDumpFilesHappy() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);

         assertThat(dump._dumpFile).exists();
         assertThat(dump._metaFile).exists();

         dump.add(new Bean(1));
         dump.add(new Bean(2));
         dump.add(new Bean(3));
         dump.delete(0);
         assertThat(dump._deletionsFile).exists();

         dump.close();
         DumpUtils.deleteDumpFiles(dump);
         assertThat(dump._dumpFile).doesNotExist();
         assertThat(dump._metaFile).doesNotExist();
         assertThat(dump._deletionsFile).doesNotExist();
         assertThat(tmpDir.listFiles()).isEmpty();
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testDeleteDumpFilesUnclosed() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
         try {
            DumpUtils.deleteDumpFiles(dump);
            fail("IllegalArgumentException expected");
         }
         catch ( IllegalArgumentException e ) {
            assertThat(e.getMessage()).contains("dump wasn't closed");
         }
         dump.close();
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testDeleteDumpIndexFiles() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
         UniqueIndex<Bean> index = new UniqueIndex<>(dump, "_id");
         dump.add(new Bean(1));

         DumpUtils.deleteDumpIndexFiles(dump);
         assertThat(dump._indexes).isEmpty();

         assertThat(index.getLookupFile()).doesNotExist();
         assertThat(index.getMetaFile()).doesNotExist();
         assertThat(index.getUpdatesFile()).doesNotExist();

         dump.close();
         DumpUtils.deleteDumpFiles(dump);
         assertThat(tmpDir.listFiles()).isEmpty();
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testDeleteDumpIndexFilesClosed() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
         dump.close();
         try {
            DumpUtils.deleteDumpIndexFiles(dump);
            fail("IllegalArgumentException expected");
         }
         catch ( IllegalArgumentException e ) {
            assertThat(e.getMessage()).contains("dump is closed");
         }
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testReadWriteUtf() throws Exception {
      StringBuilder s = new StringBuilder();
      for ( int i = 0; i < 10000; i++ ) {
         s.append("0123456789");
      }

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bos);
      DumpUtils.writeUTF(s.toString(), out);
      out.close();

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
      String ss = DumpUtils.readUTF(in);
      in.close();

      assertThat(s.toString()).isEqualTo(ss);
   }

   private Dump<Bean> createDestDump() {
      Dump<Bean> dest;
      File destFile = new File("DumpUtilsTest-destination.dmp");
      dest = new Dump<>(Bean.class, destFile);
      return dest;
   }

   private Dump<Bean> createSourceDump() throws IOException {
      Dump<Bean> source;
      File sourceFile = new File("DumpUtilsTest-source.dmp");
      source = new Dump<>(Bean.class, sourceFile);

      for ( int i = 0; i < NUMBER_OF_INSTANCES; i++ ) {
         source.add(new Bean(i));
      }
      return source;
   }

   private int deleteFromDump( Dump<Bean> source, int deleteModulo ) {
      int deletions = 0;
      for ( Bean bean : source ) {
         if ( bean._id % deleteModulo == 0 ) {
            deletions++;
            source.deleteLast();
         }
      }
      return deletions;
   }

   private void testCleanupBroken( long seekPos, String brokendata, int deleteModulo ) {
      Dump<Bean> source = null;
      Dump<Bean> dest = null;
      try {
         source = createSourceDump();
         File sourceFile = source.getDumpFile();

         int deletions = 0;
         if ( deleteModulo > 0 ) {
            deletions = deleteFromDump(source, deleteModulo);
         }

         long sourceSizeAfterDeleting = source.getDumpSize();

         DumpUtils.closeSilently(source);

         // now let's break the dump!
         RandomAccessFile raf = new RandomAccessFile(sourceFile, "rw");
         raf.seek(seekPos);
         raf.writeUTF(brokendata);
         raf.close();

         // re-open
         source = new Dump<>(Bean.class, sourceFile);

         dest = createDestDump();

         DumpUtils.cleanup(source, dest);

         long destSizeAfterCleanup = dest.getDumpSize();

         Assert.assertTrue("Dump did not shrink after cleanup", destSizeAfterCleanup < sourceSizeAfterDeleting);

         int n = 0;
         for ( Bean bean : dest ) {
            Assert.assertTrue("wrong bean data after cleanup: " + bean._id, bean._id >= 0 && bean._id < NUMBER_OF_INSTANCES);
            n++;
         }

         Assert.assertTrue("Dump did not shrink after cleanup", n < NUMBER_OF_INSTANCES);
         System.err.println(n);
         Assert.assertTrue("Dump shrank too much cleanup", n + 10 + deletions > NUMBER_OF_INSTANCES);
      }
      catch ( Exception e ) {
         e.printStackTrace();
         Assert.fail("failed to cleanup: " + e.getMessage());
      }
      finally {
         DumpUtils.closeSilently(source);
         DumpUtils.closeSilently(dest);
      }
   }
}
