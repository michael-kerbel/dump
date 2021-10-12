package util.dump.externalization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumMap;
import java.util.Objects;

import org.junit.Test;

import util.dump.ExternalizableBean;
import util.dump.stream.SingleTypeObjectInputStream;
import util.dump.stream.SingleTypeObjectOutputStream;


public class ExternalizableEnumMapTest {

   protected EnumMapContainer _beanToWrite;
   protected EnumMapContainer _beanThatWasRead;

   @Test
   public void testEnumMap() {
      EnumMap<Country, MapContent> map = new EnumMap<>(Country.class);
      map.put(Country.Germany, new MapContent("foo"));
      map.put(Country.UnitedKingdom, new MapContent("bar"));

      testBeanIsExternalizable(new EnumMapContainer(map));
   }

   protected void givenBean( EnumMapContainer beanToWrite ) {
      _beanToWrite = beanToWrite;
   }

   protected void testBeanIsExternalizable( EnumMapContainer bean ) {
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
      _beanThatWasRead = SingleTypeObjectInputStream.readSingleInstance((Class<EnumMapContainer>)_beanToWrite.getClass(), bytes);
   }

   public enum Country {
      Germany,
      UnitedKingdom
   }


   public static final class EnumMapContainer implements ExternalizableBean {

      private static final long serialVersionUID = -1996150332517358935L;

      @externalize(1)
      private EnumMap<Country, MapContent> _map;

      public EnumMapContainer() {
      }

      public EnumMapContainer( EnumMap<Country, MapContent> map ) {
         _map = map;
      }
   }


   public static final class MapContent implements ExternalizableBean {

      private static final long serialVersionUID = 1525012620308096977L;

      @externalize(1)
      private String _content;

      public MapContent() {}

      public MapContent( String content ) {
         _content = content;
      }

      @Override
      public boolean equals( Object other ) {
         if ( other instanceof MapContent ) {
            return Objects.equals(_content, ((MapContent)other)._content);
         }
         return false;
      }

      public String getContent() {
         return _content;
      }

      @Override
      public int hashCode() {
         return _content.hashCode();
      }

      public void setContent( String content ) {
         _content = content;
      }
   }
}
