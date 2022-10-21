package util.dump.externalization;

import org.junit.Test;


public class ExternalizableBeanOverloadAnnotatedGetterTest extends BaseExternalizableBeanRoundtripTest<BeanWithAnnotatedGetter> {

   @Test
   public void roundtrip() {
      testBeanIsExternalizable(new BeanWithAnnotatedGetter());
   }
}
