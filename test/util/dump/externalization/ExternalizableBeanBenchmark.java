package util.dump.externalization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import util.dump.ExternalizableBean;
import util.dump.ExternalizableBeanTest;
import util.dump.ExternalizableBeanTest.TestBean;
import util.dump.ExternalizableBeanTest.TestBeanSimple;
import util.dump.stream.SingleTypeObjectInputStream;
import util.dump.stream.SingleTypeObjectOutputStream;
import util.time.StopWatch;


public class ExternalizableBeanBenchmark {

   public static void main( String[] args ) throws Exception {
      new ExternalizableBeanBenchmark().benchmark();
   }

   private void benchmark() throws Exception {
      System.err.print("---------- instances");

      {
         Class testClass = TestBean.class;
         System.err.println(" of type " + testClass);

         Externalizable[] beans = new Externalizable[50000];
         for ( int i = 0, length = beans.length; i < length; i++ ) {
            beans[i] = ExternalizableBeanTest.newRandomInstance(testClass);
         }

         for ( int k = 0; k < 10; k++ ) {
            System.gc();

            StopWatch t = new StopWatch();

            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutput o = new SingleTypeObjectOutputStream(bo, testClass);
            for ( int j = 0, length = beans.length; j < length; j++ ) {
               beans[j].writeExternal(o);
            }
            o.close();

            ObjectInput i = new SingleTypeObjectInputStream(new ByteArrayInputStream(bo.toByteArray()), testClass);
            for ( int j = 0, length = beans.length; j < length; j++ ) {
               Externalizable dd = (Externalizable)testClass.newInstance();
               dd.readExternal(i);
               assert (dd.equals(beans[j]));
            }
            i.close();

            System.err.println("ExternalizableBean: " + t);
         }

      }

      System.err.println("---------- single-dim array of externalizables");

      {
         TestBeanSimple[][] beans = new TestBeanSimple[1000][1000];
         //      beans[i][j] = (TestBeanExternalizableBean)ExternalizableBeanTest.newRandomInstance(TestBeanExternalizableBean.class);
         //      beansSerializable[i][j] = new TestBeanSerializable(beans[i][j]);
         for ( int i = 0; i < 1000; i++ ) {
            for ( int j = 0; j < 1000; j++ ) {
               beans[i][j] = (TestBeanSimple)ExternalizableBeanTest.newRandomInstance(TestBeanSimple.class);
            }
         }

         for ( int i = 0; i < 10; i++ ) {
            StopWatch t = new StopWatch();
            for ( int j = 0, length = beans.length; j < length; j++ ) {
               ExternalizableBeanBean ExternalizableBeanBean = new ExternalizableBeanBean();
               ExternalizableBeanBean._beans = beans[j];
               byte[] b = externalize(ExternalizableBeanBean);
               ExternalizableBeanBean = new ExternalizableBeanBean();
               readExternalized(b, ExternalizableBeanBean);
            }
            System.err.println("ExternalizableBean 1-dim array: " + t);

            t = new StopWatch();
            for ( int j = 0, length = beans.length; j < length; j++ ) {
               SerializationBean serializationBean = new SerializationBean();
               serializationBean._beans = beans[j];
               byte[] b = serialize(serializationBean);
               serializationBean = (SerializationBean)readSerialized(b);
            }
            System.err.println("java Serialization 1-dim array: " + t);
         }
      }

      System.err.println("---------- two-dim array of externalizables");
      {
         TestBeanSimple[][] beans = new TestBeanSimple[1000][1000];
         //      beans[i][j] = (TestBeanExternalizableBean)ExternalizableBeanTest.newRandomInstance(TestBeanExternalizableBean.class);
         //      beansSerializable[i][j] = new TestBeanSerializable(beans[i][j]);
         for ( int i = 0; i < 1000; i++ ) {
            for ( int j = 0; j < 1000; j++ ) {
               beans[i][j] = (TestBeanSimple)ExternalizableBeanTest.newRandomInstance(TestBeanSimple.class);
            }
         }

         for ( int i = 0; i < 10; i++ ) {
            StopWatch t = new StopWatch();
            ExternalizableBeanBean2dim ExternalizableBeanBean = new ExternalizableBeanBean2dim();
            ExternalizableBeanBean._beans = beans;
            byte[] b = externalize(ExternalizableBeanBean);
            ExternalizableBeanBean = new ExternalizableBeanBean2dim();
            readExternalized(b, new ExternalizableBeanBean2dim());
            System.err.println("ExternalizableBean 2-dim array: " + t);

            t = new StopWatch();
            SerializationBean2dim serializationBean = new SerializationBean2dim();
            serializationBean._beans = beans;
            b = serialize(serializationBean);
            serializationBean = (SerializationBean2dim)readSerialized(b);
            System.err.println("java Serialization 2-dim array: " + t);
         }
      }

      System.err.println("---------- list of externalizables");
      {
         {
            List<TestBeanSimple>[] beans = new List[1000];
            //      beans[i][j] = (TestBeanExternalizableBean)ExternalizableBeanTest.newRandomInstance(TestBeanExternalizableBean.class);
            //      beansSerializable[i][j] = new TestBeanSerializable(beans[i][j]);
            for ( int i = 0; i < 1000; i++ ) {
               beans[i] = new ArrayList<TestBeanSimple>();
               for ( int j = 0; j < 1000; j++ ) {
                  beans[i].add((TestBeanSimple)ExternalizableBeanTest.newRandomInstance(TestBeanSimple.class));
               }
            }

            for ( int i = 0; i < 10; i++ ) {
               StopWatch t = new StopWatch();
               for ( int j = 0, length = beans.length; j < length; j++ ) {
                  ExternalizableBeanBeanList ExternalizableBeanBean = new ExternalizableBeanBeanList();
                  ExternalizableBeanBean._beans = beans[j];
                  byte[] b = externalize(ExternalizableBeanBean);
                  ExternalizableBeanBean = new ExternalizableBeanBeanList();
                  readExternalized(b, ExternalizableBeanBean);
               }
               System.err.println("ExternalizableBean list: " + t);

               t = new StopWatch();
               for ( int j = 0, length = beans.length; j < length; j++ ) {
                  SerializationBeanList serializationBean = new SerializationBeanList();
                  serializationBean._beans = beans[j];
                  byte[] b = serialize(serializationBean);
                  serializationBean = (SerializationBeanList)readSerialized(b);
               }
               System.err.println("java Serialization list: " + t);
            }
         }
      }

   }

   private byte[] externalize( ExternalizableBean e ) throws Exception {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream o = new ObjectOutputStream(bout);
      e.writeExternal(o);
      o.close();
      return bout.toByteArray();
   }

   private ExternalizableBean readExternalized( byte[] b, ExternalizableBean e ) throws Exception {
      ByteArrayInputStream bin = new ByteArrayInputStream(b);
      ObjectInput i = new ObjectInputStream(bin);
      e.readExternal(i);
      i.close();
      return e;
   }

   private Object readSerialized( byte[] b ) throws Exception {
      ByteArrayInputStream bin = new ByteArrayInputStream(b);
      ObjectInput i = new ObjectInputStream(bin);
      Object o = i.readObject();
      i.close();
      return o;
   }

   private byte[] serialize( Object o ) throws Exception {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutput oos = new ObjectOutputStream(bout);
      oos.writeObject(o);
      oos.close();
      return bout.toByteArray();
   }


   public static class ExternalizableBeanBean implements ExternalizableBean {

      @externalize(1)
      public TestBeanSimple[] _beans;
   }

   public static class ExternalizableBeanBean2dim implements ExternalizableBean {

      @externalize(1)
      public TestBeanSimple[][] _beans;
   }

   public static class ExternalizableBeanBeanList implements ExternalizableBean {

      @externalize(1)
      public List<TestBeanSimple> _beans;
   }

   public static class SerializationBean implements Serializable {

      private static final long serialVersionUID = -1741743127544910454L;
      public TestBeanSimple[]   _beans;
   }

   public static class SerializationBean2dim implements Serializable {

      private static final long serialVersionUID = 1600762672331989544L;
      public TestBeanSimple[][] _beans;
   }

   public static class SerializationBeanList implements Serializable {

      private static final long   serialVersionUID = -4521752962217897005L;
      public List<TestBeanSimple> _beans;
   }

   public static class TestBeanExternalizableBean implements ExternalizableBean {

      // the member vars get initialized randomly only if the field is public - a limitation of this testcase

      @externalize(1)
      public Integer _int;
      @externalize(2)
      public Boolean _booleanPrimitive;
      @externalize(3)
      public Byte    _bytePrimitive;
   }

   public static class TestBeanSerializable implements Serializable {

      private static final long serialVersionUID = 6187967184738411300L;
      public Integer            _int;
      public Boolean            _booleanPrimitive;
      public Byte               _bytePrimitive;


      TestBeanSerializable( TestBeanExternalizableBean b ) {
         _int = b._int;
         _booleanPrimitive = b._booleanPrimitive;
         _bytePrimitive = b._bytePrimitive;
      }
   }
}
