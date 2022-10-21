package util.dump.externalization;

import static org.assertj.core.api.Assertions.assertThat;

import util.dump.ExternalizableBean;
import util.dump.stream.SingleTypeObjectInputStream;
import util.dump.stream.SingleTypeObjectOutputStream;


public abstract class BaseExternalizableBeanRoundtripTest<Bean extends ExternalizableBean> {

   protected Bean _beanToWrite;
   protected Bean _beanThatWasRead;

   protected void givenBean( Bean beanToWrite ) {
      _beanToWrite = beanToWrite;
   }

   protected void testBeanIsExternalizable( Bean bean ) {
      givenBean(bean);
      whenBeanIsExternalizedAndRead();
      thenBeansAreEqual();
   }

   protected void thenBeansAreEqual() {
      assertThat(_beanThatWasRead).usingRecursiveComparison().isEqualTo(_beanToWrite);
   }

   @SuppressWarnings("unchecked")
   protected void whenBeanIsExternalizedAndRead() {
      byte[] bytes = SingleTypeObjectOutputStream.writeSingleInstance(_beanToWrite);
      _beanThatWasRead = SingleTypeObjectInputStream.readSingleInstance((Class<Bean>)_beanToWrite.getClass(), bytes);
   }

}
