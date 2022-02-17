package util.dump;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import util.io.IOUtils;


@Ignore
public class IndexSizeTest {

   private static       File   _tmpdir;
   private static final String DUMP_FILENAME = "DumpTest.dmp";
   private static final int    NUM_ITEMS     = 20;

   @BeforeClass
   public static void setUpTmpdir() throws IOException {
      _tmpdir = new File("target", "tmp");
      _tmpdir.mkdirs();
      if ( !_tmpdir.isDirectory() ) {
         throw new IOException("unable to create temporary directory: " + _tmpdir.getAbsolutePath());
      }
      System.setProperty("java.io.tmpdir", _tmpdir.getAbsolutePath());
   }

   private Random _random = new Random();

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = _tmpdir.listFiles(f -> f.getName().startsWith("DumpTest."));
      if ( dumpFile != null ) {
         for ( File df : dumpFile ) {
            if ( df.isDirectory() ) {
               IOUtils.deleteDir(df);
            } else if ( !df.delete() ) {
               System.out.println("Failed to delete old dump file " + df);
            }
         }
      }
   }

   @Test
   public void test() throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      File indexDir = new File(_tmpdir, DUMP_FILENAME + ".search.index");

      try (Dump<IndexSizeTest.Bean> dump = new Dump<>(IndexSizeTest.Bean.class, dumpFile)) {

         SearchIndex<IndexSizeTest.Bean> index = SearchIndex.with(dump, "_idLong", ( doc, o ) -> doc.add(new TextField("data", o._data, Field.Store.NO)))
               .build();

         for ( int i = 0; i < NUM_ITEMS; i++ ) {
            dump.add(new Bean(i, randomString()));
            index.commit(); // force commit
         }

         long dirSize = dirSize(indexDir);
         System.err.println("index-size:" + dirSize);

         for ( Bean b : dump ) {
            b._data = randomString();
            dump.updateLast(b);
            index.commit(); // force commit
         }

         dirSize = dirSize(indexDir);
         System.err.println("index-size:" + dirSize);
      }
      System.err.println("index-size:" + dirSize(indexDir));
      try (Dump<IndexSizeTest.Bean> dump = new Dump<>(IndexSizeTest.Bean.class, dumpFile)) {
         SearchIndex<IndexSizeTest.Bean> index = SearchIndex.with(dump, "_idLong", ( doc, o ) -> doc.add(new TextField("data", o._data, Field.Store.NO)))
               .build();
         long dirSize = dirSize(indexDir);
         System.err.println("index-size:" + dirSize);
      }
   }

   private long dirSize( File indexDir ) {
      return Arrays.stream(Objects.requireNonNull(indexDir.listFiles())).mapToLong(File::length).sum();
   }

   private String randomString() {
      int length = _random.nextInt(100) + 1;
      return _random.ints(32, 123).limit(length).collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
   }

   public static class Bean implements ExternalizableBean {

      @externalize(1)
      long   _idLong;
      @externalize(10)
      String _data;

      public Bean() {
         // for Externalization
      }

      public Bean( int id, String data ) {
         _idLong = id;
         _data = data;
      }

      @Override
      public boolean equals( Object o ) {
         if ( this == o ) {
            return true;
         }
         if ( o == null || getClass() != o.getClass() ) {
            return false;
         }
         Bean bean = (Bean)o;
         return _idLong == bean._idLong && Objects.equals(_data, bean._data);
      }

      @Override
      public int hashCode() {
         return Objects.hash(_idLong, _data);
      }
   }

}
