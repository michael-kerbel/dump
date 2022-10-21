package util.dump.externalization;

import org.junit.Test;


public class ExternalizableBeanOverloadAnnotatedSetterTest extends BaseExternalizableBeanRoundtripTest<BeanWithAnnotatedSetter> {

   @Test
   public void roundtrip() {
      testBeanIsExternalizable(new BeanWithAnnotatedSetter());
   }
}
