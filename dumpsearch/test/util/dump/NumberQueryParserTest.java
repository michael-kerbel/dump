package util.dump;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Test;

import util.dump.sort.TempFileProvider;
import util.reflection.FieldFieldAccessor;
import util.reflection.Reflection;


public class NumberQueryParserTest {

   @Test
   public void test() throws IOException, NoSuchFieldException, ParseException {
      TempFileProvider tempFileProvider = new TempFileProvider(new File("target"));
      try (Dump<Bean> dump = new Dump<>(Bean.class, tempFileProvider.getNextTemporaryFile())) {

         SearchIndex<Bean> index = SearchIndex.with(dump, "_id", ( doc, b ) -> {
            doc.add(new LongPoint("longField", b._longField));
            doc.add(new DoublePoint("doubleField", b._doubleField));
            doc.add(new TextField("text", b._text, Field.Store.NO));
         }).withLongFields("longField").withDoubleFields("doubleField").build();

         Bean firstBean = new Bean(1, "first text", 1L, 0.1);
         dump.add(firstBean);
         Bean secondBean = new Bean(2, "second text", 2L, 0.2);
         dump.add(secondBean);
         Bean thirdBean = new Bean(3, "third text", 3L, 0.3);
         dump.add(thirdBean);

         List<Bean> beans = search(index, "longField:[0 TO 1]");
         assertSingleResult(beans, firstBean);

         beans = search(index, "longField:[0 TO 3] text:third");
         assertSingleResult(beans, thirdBean);

         beans = search(index, "doubleField:[0.25 TO 1]");
         assertSingleResult(beans, thirdBean);

         beans = search(index, "longField:2");
         assertSingleResult(beans, secondBean);

         beans = search(index, "doubleField:0.2");
         assertSingleResult(beans, secondBean);
      }
   }

   private void assertSingleResult( List<Bean> beans, Bean firstBean ) {
      assertThat(beans.isEmpty()).as("query did not find item").isFalse();
      assertThat(beans.size()).as("query found too many items").isEqualTo(1);
      assertThat(beans.get(0)).as("query found wrong item").isEqualTo(firstBean);
   }

   private List<Bean> search( SearchIndex<Bean> index, String query ) throws org.apache.lucene.queryparser.classic.ParseException, IOException {
      Iterable<Bean> result = index.search(query);
      return StreamSupport.stream(result.spliterator(), false).collect(Collectors.toList());
   }


   public static class Bean implements ExternalizableBean {

      @externalize(1)
      private int    _id;
      @externalize(2)
      private String _text;
      @externalize(3)
      private long   _longField;
      @externalize(4)
      private double _doubleField;


      public Bean() {}

      public Bean( int id, String text, long longField, double doubleField ) {
         _id = id;
         _text = text;
         _longField = longField;
         _doubleField = doubleField;
      }

      @Override
      public boolean equals( Object o ) {
         if ( this == o ) {
            return true;
         }
         if ( o == null || getClass() != o.getClass() ) {
            return false;
         }

         Bean bean = (Bean)o;

         if ( _id != bean._id ) {
            return false;
         }
         if ( _longField != bean._longField ) {
            return false;
         }
         if ( Double.compare(bean._doubleField, _doubleField) != 0 ) {
            return false;
         }
         return _text != null ? _text.equals(bean._text) : bean._text == null;
      }

      @Override
      public int hashCode() {
         int result;
         long temp;
         result = _id;
         result = 31 * result + (_text != null ? _text.hashCode() : 0);
         result = 31 * result + (int)(_longField ^ (_longField >>> 32));
         temp = Double.doubleToLongBits(_doubleField);
         result = 31 * result + (int)(temp ^ (temp >>> 32));
         return result;
      }
   }
}