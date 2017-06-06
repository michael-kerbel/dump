package util.dump.stream;

import static org.fest.assertions.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import util.dump.ExternalizableBean;


public class ExternalizableObjectStreamProviderTest {

   @Test
   public void test() throws Exception {
      ExternalizableObjectStreamProvider provider = new ExternalizableObjectStreamProvider();

      test(provider);
   }

   @Test
   public void testCompression() throws Exception {
      ExternalizableObjectStreamProvider provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel0);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel1);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel2);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel3);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel4);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel5);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel6);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel7);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel8);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.GZipLevel9);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.Snappy);
      test(provider);
      provider = new ExternalizableObjectStreamProvider(Compression.LZ4);
      test(provider);

      SingleTypeObjectStreamProvider<TestExternalizableBean> stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(
         TestExternalizableBean.class, Compression.GZipLevel0);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel1);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel2);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel3);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel4);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel5);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel6);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel7);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel8);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.GZipLevel9);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.Snappy);
      test(stProvider);
      stProvider = new SingleTypeObjectStreamProvider<TestExternalizableBean>(TestExternalizableBean.class, Compression.LZ4);
      test(stProvider);
   }

   protected void test( SingleTypeObjectStreamProvider<TestExternalizableBean> provider ) throws IOException, ClassNotFoundException {
      TestExternalizableBean bean = new TestExternalizableBean();

      testInstance(provider, bean);

      bean._d = 1d;
      bean._date = new Date();
      bean._f = 1f;
      bean._i = 1;

      testInstance(provider, bean);

      bean._l = 1L;
      bean._s = "11376136";

      testInstance(provider, bean);
   }

   protected void test( ExternalizableObjectStreamProvider provider ) throws IOException, ClassNotFoundException {
      TestBean bean = new TestBean();

      testInstance(provider, bean);

      bean._d = 1d;
      bean._date = new Date();
      bean._f = 1f;
      bean._i = 1;
      bean._o = new TestBeanNonExternalizable();

      testInstance(provider, bean);

      bean._l = 1L;
      bean._s = "1";
      bean._u = UUID.randomUUID();
      bean._o = new TestBeanNonExternalizable();
      bean._b = new TestBean();
      bean._b._d = 2d;
      bean._b._date = new Date();
      bean._b._f = 2f;
      bean._b._i = 2;
      bean._b._l = 2L;
      bean._b._s = "2";
      bean._b._u = UUID.randomUUID();

      testInstance(provider, bean);
   }

   protected void testInstance( ObjectStreamProvider provider, Object bean ) throws IOException, ClassNotFoundException {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      ObjectOutput out = provider.createObjectOutput(bytesOut);
      out.writeObject(bean);
      out.close();

      byte[] bytes = bytesOut.toByteArray();
      ObjectInput input = provider.createObjectInput(new ByteArrayInputStream(bytes));
      Object deserialized = input.readObject();

      assertThat(deserialized).isEqualTo(bean);
   }


   public static class TestExternalizableBean implements ExternalizableBean {

      @externalize(1)
      Date   _date;
      @externalize(2)
      float  _f;
      @externalize(3)
      double _d;
      @externalize(4)
      int    _i;
      @externalize(5)
      long   _l;
      @externalize(6)
      String _s;


      public TestExternalizableBean() {}

      @Override
      public boolean equals( Object obj ) {
         if ( this == obj ) {
            return true;
         }
         if ( obj == null ) {
            return false;
         }
         if ( getClass() != obj.getClass() ) {
            return false;
         }
         TestExternalizableBean other = (TestExternalizableBean)obj;
         if ( Double.doubleToLongBits(_d) != Double.doubleToLongBits(other._d) ) {
            return false;
         }
         if ( _date == null ) {
            if ( other._date != null ) {
               return false;
            }
         } else if ( !_date.equals(other._date) ) {
            return false;
         }
         if ( Float.floatToIntBits(_f) != Float.floatToIntBits(other._f) ) {
            return false;
         }
         if ( _i != other._i ) {
            return false;
         }
         if ( _l != other._l ) {
            return false;
         }
         if ( _s == null ) {
            if ( other._s != null ) {
               return false;
            }
         } else if ( !_s.equals(other._s) ) {
            return false;
         }
         return true;
      }

   }

   public static class TestBean implements Externalizable {

      Date     _date;
      UUID     _u;
      Float    _f;
      Double   _d;
      Integer  _i;
      Long     _l;
      String   _s;
      TestBean _b;
      Object   _o;


      @Override
      public boolean equals( Object obj ) {
         if ( this == obj ) {
            return true;
         }
         if ( obj == null ) {
            return false;
         }
         if ( getClass() != obj.getClass() ) {
            return false;
         }
         TestBean other = (TestBean)obj;
         if ( _b == null ) {
            if ( other._b != null ) {
               return false;
            }
         } else if ( !_b.equals(other._b) ) {
            return false;
         }
         if ( _d == null ) {
            if ( other._d != null ) {
               return false;
            }
         } else if ( !_d.equals(other._d) ) {
            return false;
         }
         if ( _date == null ) {
            if ( other._date != null ) {
               return false;
            }
         } else if ( !_date.equals(other._date) ) {
            return false;
         }
         if ( _f == null ) {
            if ( other._f != null ) {
               return false;
            }
         } else if ( !_f.equals(other._f) ) {
            return false;
         }
         if ( _i == null ) {
            if ( other._i != null ) {
               return false;
            }
         } else if ( !_i.equals(other._i) ) {
            return false;
         }
         if ( _l == null ) {
            if ( other._l != null ) {
               return false;
            }
         } else if ( !_l.equals(other._l) ) {
            return false;
         }
         if ( _s == null ) {
            if ( other._s != null ) {
               return false;
            }
         } else if ( !_s.equals(other._s) ) {
            return false;
         }
         if ( _u == null ) {
            if ( other._u != null ) {
               return false;
            }
         } else if ( !_u.equals(other._u) ) {
            return false;
         }
         if ( _o == null ) {
            if ( other._o != null ) {
               return false;
            }
         } else if ( !_o.equals(other._o) ) {
            return false;
         }
         return true;
      }

      @Override
      public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
         _date = (Date)in.readObject();
         _u = (UUID)in.readObject();
         _f = (Float)in.readObject();
         _d = (Double)in.readObject();
         _i = (Integer)in.readObject();
         _f = (Float)in.readObject();
         _l = (Long)in.readObject();
         _s = (String)in.readObject();
         _b = (TestBean)in.readObject();
         _o = in.readObject();
      }

      @Override
      public void writeExternal( java.io.ObjectOutput out ) throws IOException {
         out.writeObject(_date);
         out.writeObject(_u);
         out.writeObject(_f);
         out.writeObject(_d);
         out.writeObject(_i);
         out.writeObject(_f);
         out.writeObject(_l);
         out.writeObject(_s);
         out.writeObject(_b);
         out.writeObject(_o);
      }
   }

   public static class TestBeanNonExternalizable implements Serializable {

      double d = Math.random();


      @Override
      public boolean equals( Object obj ) {
         return ((TestBeanNonExternalizable)obj).d == d;
      }
   }

}
