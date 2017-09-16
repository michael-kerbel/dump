package util.dump;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class UniqueIndexWithLongPayloadTest {

   private static final String DUMP_FILENAME = "UniqueIndexWithLongPayloadTest.dmp";
   private static File         _tmpdir;


   @BeforeClass
   public static void setUpTmpdir() throws IOException {
      _tmpdir = new File("target", "tmp");
      _tmpdir.mkdirs();
      if ( !_tmpdir.isDirectory() ) {
         throw new IOException("unable to create temporary directory: " + _tmpdir.getAbsolutePath());
      }
      System.setProperty("java.io.tmpdir", _tmpdir.getAbsolutePath());
   }

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = _tmpdir.listFiles(f -> f.getName().startsWith("UniqueIndexWithLongPayloadTest."));
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
         UniqueIndexWithLongPayload<UniqueIndexTest.Bean> intIndex = new UniqueIndexWithLongPayload<>(dump, "_idInt", b -> b._idLongObject);
         UniqueIndexWithLongPayload<UniqueIndexTest.Bean> longIndex = new UniqueIndexWithLongPayload<>(dump, "_idLong", b -> b._idLongObject);
         UniqueIndexWithLongPayload<UniqueIndexTest.Bean> stringIndex = new UniqueIndexWithLongPayload<>(dump, "_idString", b -> b._idLongObject);

         int numBeansToAddForTest = 500;
         for ( int i = 0; i < numBeansToAddForTest; i++ ) {
            dump.add(new UniqueIndexTest.Bean(i, null));
         }

         assertPayload(dump, intIndex, longIndex, stringIndex);

         // reopen dump
         dump.close();
         dump = new Dump<>(UniqueIndexTest.Bean.class, dumpFile);
         intIndex = new UniqueIndexWithLongPayload<>(dump, "_idInt", b -> b._idLongObject);
         longIndex = new UniqueIndexWithLongPayload<>(dump, "_idLong", b -> b._idLongObject);
         stringIndex = new UniqueIndexWithLongPayload<>(dump, "_idString", b -> b._idLongObject);

         int deleted = 0;
         for ( UniqueIndexTest.Bean bean : dump ) {
            if ( bean._idInt % 2 == 0 ) {
               dump.deleteLast();
               deleted++;
            } else {
               bean._idLongObject = bean._idLongObject * 2;
               dump.updateLast(bean);
            }
         }

         for ( UniqueIndexTest.Bean bean : dump ) {
            if ( bean._idInt % 5 == 0 ) {
               bean._idLongObject = bean._idLongObject * 2;
               dump.updateLast(bean);
            } else {
               dump.updateLast(bean);
            }
         }

         assertPayload(dump, intIndex, longIndex, stringIndex);

         // reopen dump
         dump.close();
         dump = new Dump<>(UniqueIndexTest.Bean.class, dumpFile);
         intIndex = new UniqueIndexWithLongPayload<>(dump, "_idInt", b -> b._idLongObject);
         longIndex = new UniqueIndexWithLongPayload<>(dump, "_idLong", b -> b._idLongObject);
         stringIndex = new UniqueIndexWithLongPayload<>(dump, "_idString", b -> b._idLongObject);

         assertPayload(dump, intIndex, longIndex, stringIndex);
      }
      finally {
         DumpUtils.closeSilently(dump);
      }
   }

   private void assertPayload( Dump<UniqueIndexTest.Bean> dump, UniqueIndexWithLongPayload<UniqueIndexTest.Bean> intIndex,
         UniqueIndexWithLongPayload<UniqueIndexTest.Bean> longIndex, UniqueIndexWithLongPayload<UniqueIndexTest.Bean> stringIndex ) {
      for ( UniqueIndexTest.Bean b : dump ) {
         Assert.assertEquals("payload not correct", b._idLongObject.longValue(), intIndex.lookupPayload(b._idInt));
         Assert.assertEquals("payload not correct", b._idLongObject.longValue(), longIndex.lookupPayload(b._idLong));
         Assert.assertEquals("payload not correct", b._idLongObject.longValue(), stringIndex.lookupPayload(b._idString));
      }
   }
}