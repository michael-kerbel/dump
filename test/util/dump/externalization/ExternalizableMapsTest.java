package util.dump.externalization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Test;

import util.dump.ExternalizableBean;
import util.dump.stream.SingleTypeObjectInputStream;
import util.dump.stream.SingleTypeObjectOutputStream;


public class ExternalizableMapsTest {

   protected MapContainer _beanToWrite;
   protected MapContainer _beanThatWasRead;

   @Test
   public void testConcurrentHashMap() {
      testBeanIsExternalizable(new MapContainer(new ConcurrentHashMap<>(Map.of("foo", new MapContent("foo"), "bar", new MapContent("bar")))));
   }

   @Test
   public void testHashMap() {
      testBeanIsExternalizable(new MapContainer(new HashMap<>(Map.of("foo", new MapContent("foo"), "bar", new MapContent("bar")))));
   }

   @Test
   public void testImmutableMap1() {
      testBeanIsExternalizable(new MapContainer(Map.of("foo", new MapContent("foo"))));
   }

   @Test
   public void testImmutableMapN() {
      testBeanIsExternalizable(new MapContainer(Map.of("foo", new MapContent("foo"), "bar", new MapContent("bar"))));
   }

   @Test
   public void testTreeMap() {
      testBeanIsExternalizable(new MapContainer(new TreeMap<>(Map.of("foo", new MapContent("foo"), "bar", new MapContent("bar")))));
   }

   @Test
   public void testUnmodifiableMap() {
      testBeanIsExternalizable(
            new MapContainer(Collections.unmodifiableMap(new HashMap<>(Map.of("foo", new MapContent("foo"), "bar", new MapContent("bar"))))));
   }

   protected void givenBean( MapContainer beanToWrite ) {
      _beanToWrite = beanToWrite;
   }

   protected void testBeanIsExternalizable( MapContainer bean ) {
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
      _beanThatWasRead = SingleTypeObjectInputStream.readSingleInstance((Class<MapContainer>)_beanToWrite.getClass(), bytes);
   }

   public static final class MapContainer implements ExternalizableBean {

      private static final long serialVersionUID = -1996150332517358935L;

      @externalize(1)
      private Map<String, MapContent> _map;

      public MapContainer() {
      }

      public MapContainer( Map<String, MapContent> map ) {
         _map = map;
      }

      public Map<String, MapContent> getMap() {
         return _map;
      }

      public void setMap( Map<String, MapContent> map ) {
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