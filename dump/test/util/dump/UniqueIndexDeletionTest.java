package util.dump;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class UniqueIndexDeletionTest {

   private static final String DUMP_FILENAME = "UniqueIndexDeletionTest.dmp";
   private static       File   _tmpdir;

   @BeforeClass
   public static void setUpTmpdir() throws IOException {
      _tmpdir = new File("target", "tmp");
      _tmpdir.mkdirs();
      if ( !_tmpdir.exists() ) {
         throw new IOException("unable to create temporary directory: " + _tmpdir.getAbsolutePath());
      }
      System.setProperty("java.io.tmpdir", _tmpdir.getAbsolutePath());
   }

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = _tmpdir.listFiles(f -> f.getName().startsWith("UniqueIndexDeletionTest."));
      if ( dumpFile != null ) {
         for ( File df : dumpFile ) {
            if ( !df.delete() ) {
               System.out.println("Failed to delete old dump file " + df);
            }
         }
      }
   }

   @Test
   public void test() throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);
      Dump<UniqueIndexTest.Bean> dump = null;
      try {
         dump = new Dump<>(UniqueIndexTest.Bean.class, dumpFile);
         UniqueIndex<UniqueIndexTest.Bean> intIndex = new UniqueIndex<>(dump, "_idInt");
         UniqueIndex<UniqueIndexTest.Bean> stringIndex = new UniqueIndex<>(dump, "_idString");

         dump.add(new UniqueIndexTest.Bean(1, "data"));
         dump.add(new UniqueIndexTest.Bean(2, "data"));

         UniqueIndexTest.Bean bean = intIndex.lookup(1);
         assertThat(bean).as("bean 1 not found in index").isNotNull();
         bean = stringIndex.lookup("+1");
         assertThat(bean).as("bean 1 not found in index").isNotNull();
         bean = dump.deleteLast();
         assertThat(bean).as("failed to delete bean").isNotNull();
         bean = intIndex.lookup(1);
         assertThat(bean).as("bean 1 not deleted in index").isNull();
         bean = stringIndex.lookup("+1");
         assertThat(bean).as("bean 1 not deleted in index").isNull();

         dump.close();
         Arrays.stream(Objects.requireNonNull(_tmpdir.listFiles(s -> s.getName().contains("_idString")))).forEach(File::delete);

         dump = new Dump<>(UniqueIndexTest.Bean.class, dumpFile);
         intIndex = new UniqueIndex<>(dump, "_idInt");
         stringIndex = new UniqueIndex<>(dump, "_idString");

         bean = intIndex.lookup(1);
         assertThat(bean).as("bean 1 not deleted in index").isNull();
         bean = stringIndex.lookup("+1");
         assertThat(bean).as("bean 1 not deleted in index").isNull();

         dump.add(new UniqueIndexTest.Bean(1, "data"));
         bean = intIndex.lookup(1);
         assertThat(bean).as("bean 1 not found in index").isNotNull();
         bean = stringIndex.lookup("+1");
         assertThat(bean).as("bean 1 not found in index").isNotNull();

      }
      finally {
         DumpUtils.closeSilently(dump);
      }

   }

}
