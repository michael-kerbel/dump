package util.dump.externalization;

import java.nio.charset.StandardCharsets;

import util.dump.ExternalizableBean;


public class BeanWithAnnotatedGetter implements ExternalizableBean {

   @externalize(2)
   private String _value = "1349";

   public BeanWithAnnotatedGetter() {}

   @externalize(1)
   public String getValue() {
      return getValueAsString();
   }

   public byte[] getValueAsBytes() {
      return _value.getBytes(StandardCharsets.UTF_8);
   }

   public String getValueAsString() {
      return _value;
   }

   public void setValue( byte[] v ) {
      _value = new String(v, StandardCharsets.UTF_8);
   }

   public void setValue( String v ) {
      _value = v;
   }
}
