package util.dump;

import java.io.File;
import java.io.FileFilter;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.time.StopWatch;


public class MultithreadedDumpReaderTest {

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(new FileFilter() {

         @Override
         public boolean accept( File f ) {
            return f.getName().startsWith("DumpTest");
         }
      });
      for ( File df : dumpFile ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Test
   public void testDeletion() throws Exception {
      File dumpFile = new File("DumpTest-deletions.dmp");
      Dump<TestBean> dump = new Dump<TestBean>(TestBean.class, dumpFile);
      UniqueIndex<TestBean> index = new UniqueIndex<TestBean>(dump, "_int");
      for ( int i = 0; i < 300000; i++ ) {
         TestBean e = (TestBean)ExternalizableBeanTest.newRandomInstance(TestBean.class);
         e._int = i;
         dump.add(e);
      }

      int i = 0;
      for ( TestBean b : dump ) {
         if ( i % 10 != 9 ) {
            dump.deleteLast();
         }
         i++;
      }
      dump.close();

      dump = new Dump<TestBean>(TestBean.class, dumpFile);
      index = new UniqueIndex<TestBean>(dump, "_int");
      i = 9; // 0-8 were deleted
      for ( TestBean bean : new MultithreadedDumpReader<TestBean>(dump, index) ) {
         Assert.assertNotNull("Element returned during iteration of empty dump", bean);
         Assert.assertEquals("wrong order of beans!", i, bean._int);
         i += 10;
      }

      dump.close();
   }

   @Test
   public void test() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      Dump<TestBean> dump = new Dump<TestBean>(TestBean.class, dumpFile);
      try {
         UniqueIndex<TestBean> index = new UniqueIndex<TestBean>(dump, "_int");
         for ( int i = 0; i < 300000; i++ ) {
            TestBean e = (TestBean)ExternalizableBeanTest.newRandomInstance(TestBean.class);
            e._int = i;
            dump.add(e);
         }
         dump.close();

         dump = new Dump<TestBean>(TestBean.class, dumpFile);

         index = new UniqueIndex<TestBean>(dump, "_int");

         for ( int j = 0; j < 10; j++ ) {

            StopWatch t = new StopWatch();
            int i = 0;
            //         for ( TestBean bean : dump ) {
            //            i++;
            //         }
            //         System.err.println(i + " instances iterated in single thread in " + t);
            //
            //         t = new StopWatch();
            //         i = 0;
            for ( TestBean bean : new MultithreadedDumpReader<TestBean>(dump, index) ) {
               Assert.assertNotNull("Element returned during iteration of empty dump", bean);
               Assert.assertEquals("wrong order of beans!", i++, bean._int);
            }
            System.err.println(i + " instances iterated in multiple threads in " + t);

         }
      }
      finally {
         DumpUtils.closeSilently(dump);
      }
   }


   public static class TestBean implements ExternalizableBean {

      // this member var gets initialized randomly only if the field is public - a limitation of this testcase

      public int     _int;
      public boolean _booleanPrimitive;
      public byte    _bytePrimitive;
      public char    _char;
      public double  _doublePrimitive;
      public float   _floatPrimitive;
      public long    _longPrimitive;
      public short   _shortPrimitive;
      public String  _string;
      @externalize(50)
      public String  _string2;
      @externalize(51)
      public String  _string3;
      @externalize(52)
      public String  _string4;
      @externalize(53)
      public String  _string5;


      @externalize(23)
      public byte getBytePrimitive2() {
         return _bytePrimitive;
      }

      @externalize(4)
      public char getChar() {
         return _char;
      }

      @externalize(8)
      public double getDoublePrimitive() {
         return _doublePrimitive;
      }

      @externalize(10)
      public float getFloatPrimitive() {
         return _floatPrimitive;
      }

      @externalize(11)
      public int getInt() {
         return _int;
      }

      @externalize(14)
      public long getLongPrimitive() {
         return _longPrimitive;
      }

      @externalize(16)
      public short getShortPrimitive() {
         return _shortPrimitive;
      }

      @externalize(17)
      public String getString() {
         return _string;
      }

      @externalize(18)
      public boolean isBooleanPrimitive() {
         return _booleanPrimitive;
      }

      public void setBooleanPrimitive( boolean booleanPrimitive ) {
         _booleanPrimitive = booleanPrimitive;
      }

      public void setBytePrimitive2( byte bytePrimitive ) {
         _bytePrimitive = bytePrimitive;
      }

      public void setChar( char c ) {
         _char = c;
      }

      public void setDoublePrimitive( double doublePrimitive ) {
         _doublePrimitive = doublePrimitive;
      }

      public void setFloatPrimitive( float floatPrimitive ) {
         _floatPrimitive = floatPrimitive;
      }

      public void setInt( int i ) {
         _int = i;
      }

      public void setLongPrimitive( long longPrimitive ) {
         _longPrimitive = longPrimitive;
      }

      public void setShortPrimitive( short shortPrimitive ) {
         _shortPrimitive = shortPrimitive;
      }

      public void setString( String string ) {
         _string = string;
      }
   }

}
