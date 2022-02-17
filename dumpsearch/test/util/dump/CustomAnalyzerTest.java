package util.dump;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Test;

import util.dump.sort.TempFileProvider;


public class CustomAnalyzerTest {

   @Test
   public void test() throws IOException, NoSuchFieldException, ParseException {
      TempFileProvider tempFileProvider = new TempFileProvider(new File("target"));
      try (Dump<Bean> dump = new Dump<>(Bean.class, tempFileProvider.getNextTemporaryFile())) {

         CustomAnalyzer analyzer = CustomAnalyzer.builder() //
               .withTokenizer(StandardTokenizerFactory.class) //
               .addTokenFilter(LowerCaseFilterFactory.class).addTokenFilter(NGramFilterFactory.class, "minGramSize", "1", "maxGramSize", "100").build();
         IndexWriterConfig config = new IndexWriterConfig(analyzer);

         SearchIndex<Bean> index = SearchIndex.with(dump, "_id", ( doc, b ) -> {
            doc.add(new TextField("text", b._text, Field.Store.NO));
         }).withIndexWriterConfig(config).build();

         Bean firstBean = new Bean(1, "first text");
         dump.add(firstBean);
         Bean secondBean = new Bean(2, "second text");
         dump.add(secondBean);
         Bean thirdBean = new Bean(3, "third text");
         dump.add(thirdBean);

         List<Bean> beans = search(index, "text:thir");
         assertSingleResult(beans, thirdBean);

         beans = search(index, "text:hir");
         assertSingleResult(beans, thirdBean);
      }
   }

   private void assertSingleResult( List<Bean> beans, Bean firstBean ) {
      assertThat(beans.isEmpty()).as("query did not find item").isFalse();
      assertThat(beans.size()).as("query found too many items").isEqualTo(1);
      assertThat(beans.get(0)).as("query found wrong item").isEqualTo(firstBean);
   }

   private List<Bean> search( SearchIndex<Bean> index, String query ) throws ParseException, IOException {
      Iterable<Bean> result = index.search(query);
      return StreamSupport.stream(result.spliterator(), false).collect(Collectors.toList());
   }

   public static class Bean implements ExternalizableBean {

      @externalize(1)
      private int    _id;
      @externalize(2)
      private String _text;

      public Bean() {}

      public Bean( int id, String text ) {
         _id = id;
         _text = text;
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
         return _text != null ? _text.equals(bean._text) : bean._text == null;
      }

      @Override
      public int hashCode() {
         int result;
         long temp;
         result = _id;
         result = 31 * result + (_text != null ? _text.hashCode() : 0);
         return result;
      }
   }
}