package util.dump;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.junit.Test;


/**
 * An example beside the "real" Test-Case to demonstrate the usage and benefits.
 */
public class ExternalizableBeanExample {

   @Test
   public void testWriteRead() throws IOException, ClassNotFoundException {
      BeanV1 version1 = new BeanV1();
      version1.name = "foobar";
      version1.value = 42;
      version1.oldValue = 99;

      // write
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      ObjectOutput o = new ObjectOutputStream(bo);
      version1.writeExternal(o);
      o.close();

      // contains version 1 beans
      byte[] buffer = bo.toByteArray();

      // read
      ObjectInput i = new ObjectInputStream(new ByteArrayInputStream(buffer));
      BeanV2 version2 = new BeanV2();
      version2.readExternal(i);
      i.close();

      assertThat(version1.name).isEqualTo(version2.name);
      assertThat(version1.value).isEqualTo(version2.value);
      assertThat(version1.oldValue).isEqualTo(99); // not available to version2
      assertThat(version2.newValue).isEqualTo(0);  // not present in buffer
   }


   public static class BeanV1 implements ExternalizableBean {

      @externalize(1)
      public String name;

      @externalize(2)
      public int value;

      @externalize(3)
      public int oldValue;
   }


   public static class BeanV2 implements ExternalizableBean {

      @externalize(1)
      public String name;

      @externalize(2)
      public int value;

      // not available anymore
      //@externalize(3)
      //public int oldValue;

      // new in version 2, but needs new index=4
      @externalize(4)
      public int newValue;
   }

}
