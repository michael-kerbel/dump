package util.dump;

import static org.assertj.core.api.Assertions.assertThat;
import static util.dump.SearchIndex.SortBuilder.Direction.ASC;
import static util.dump.SearchIndex.SortBuilder.Direction.DESC;
import static util.dump.SearchIndex.sort;
import static util.dump.SearchIndex.with;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import util.dump.DumpIndex.IndexMeta;
import util.dump.io.IOUtils;


public class SearchIndexTest {

   private static       File   _tmpdir;
   private static final String DUMP_FILENAME = "DumpTest.dmp";

   @BeforeClass
   public static void setUpTmpdir() throws IOException {
      _tmpdir = new File("target", "tmp");
      _tmpdir.mkdirs();
      if ( !_tmpdir.isDirectory() ) {
         throw new IOException("unable to create temporary directory: " + _tmpdir.getAbsolutePath());
      }
      System.setProperty("java.io.tmpdir", _tmpdir.getAbsolutePath());
   }

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = _tmpdir.listFiles(f -> f.getName().startsWith("DumpTest."));
      if ( dumpFile != null ) {
         for ( File df : dumpFile ) {
            if ( df.isDirectory() ) {
               IOUtils.deleteDir(df);
            } else if ( !df.delete() ) {
               System.out.println("Failed to delete old dump file " + df);
            }
         }
      }
   }

   @Test
   public void testBasicOperations() throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {

         SearchIndex<Bean> index = with(dump, "_idLong", ( doc, o ) -> {
            doc.add(new SortedDocValuesField("id", new BytesRef(o._idString)));
            doc.add(new TextField("data", o._data, Store.NO));
         }).build();

         Bean firstBean = new Bean(1, "first  row");
         dump.add(firstBean);
         Bean secondBean = new Bean(2, "second row");
         dump.add(secondBean);
         Bean thirdBean = new Bean(3, "third one");
         dump.add(thirdBean);

         assertThat(index.contains(1)).as("contains does not work").isTrue();

         List<Bean> beans = search(index, "data:first");
         assertSingleResult(beans, firstBean);

         beans = search(index, "data:second");
         assertSingleResult(beans, secondBean);

         beans = search(index, "data:row");
         assertThat(beans.isEmpty()).as("query did not find items").isFalse();
         assertThat(beans.size()).as("query did not find all items").isEqualTo(2);
         assertThat(beans.contains(firstBean)).as("query found unexpected items").isTrue();
         assertThat(beans.contains(secondBean)).as("query found unexpected items").isTrue();

         assertThat(index.countMatches("data:row")).as("count did not find all items").isEqualTo(2);

         beans = search(index, "data:third");
         assertSingleResult(beans, thirdBean);

         beans = index.searchBlocking("data:row OR data:third", 999, sort().byText("id", DESC).build());
         assertThat(beans).containsExactly(thirdBean, secondBean, firstBean);
         beans = index.searchBlocking("data:row OR data:third", 999, sort().byText("id", ASC).build());
         assertThat(beans).containsExactly(firstBean, secondBean, thirdBean);

         search(index, "data:third");
         dump.deleteLast();
         assertThat(index.getNumKeys()).as("deletion failed").isEqualTo(2);

         beans = search(index, "data:third");
         assertThat(beans).as("deletion failed").isEmpty();

         beans = search(index, "data:first");
         assertSingleResult(beans, firstBean);
         Bean updatedFirstBean = beans.get(0);
         updatedFirstBean._data = "fourth row";
         dump.updateLast(updatedFirstBean);

         beans = search(index, "data:fourth");
         assertSingleResult(beans, updatedFirstBean);
      }

      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {

         SearchIndex<Bean> index = with(dump, "_idLong", ( doc, o ) -> doc.add(new TextField("data", o._data, Store.NO))).build();

         List<Bean> beans = search(index, "data:row");
         assertThat(beans.isEmpty()).as("query did not find items").isFalse();
         assertThat(beans.size()).as("query did not find all items").isEqualTo(2);
      }
   }

   @Test
   public void testFaceting() throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         SearchIndex<Bean> index = with(dump, "_idLong", ( doc, o ) -> doc.add(new FacetField("facetField", o._data))).build();

         for ( int i = 0; i < 100; i++ ) {
            dump.add(new Bean(i, "" + i % 10));
         }

         List<FacetResult> facetResults = index.facetSearch("id:[0 TO 999]");
         assertThat(facetResults.size()).isEqualTo(1);
         FacetResult facetResult = facetResults.get(0);
         assertThat(facetResult.value).isEqualTo(100);
         assertThat(facetResult.childCount).isEqualTo(10);
         Arrays.stream(facetResult.labelValues).forEach(lav -> assertThat(lav.value).isEqualTo(10));

         for ( int i = 100; i < 110; i++ ) {
            dump.add(new Bean(i, "" + i % 10));
         }
         facetResults = index.facetSearch("id:[0 TO 999]");
         facetResult = facetResults.get(0);
         Arrays.stream(facetResult.labelValues).forEach(lav -> assertThat(lav.value).isEqualTo(11));

         Bean bean100 = index.search("id:100").iterator().next();
         bean100._data = "X";
         dump.updateLast(bean100);

         facetResults = index.facetSearch("id:[100 TO 100]");
         facetResult = facetResults.get(0);
         Arrays.stream(facetResult.labelValues).forEach(lav -> {
            assertThat(lav.label).isEqualTo("X");
            assertThat(lav.value).isEqualTo(1);
         });
      }
   }

   @Test
   public void testVersion() throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {

         SearchIndex<Bean> index = with(dump, "_idLong", ( doc, o ) -> doc.add(new TextField("data", o._data, Store.NO))).withVersion(1).build();

         Bean firstBean = new Bean(1, "first row");
         dump.add(firstBean);
         index.flushMeta();

         IndexMeta indexMeta = new IndexMeta();
         DumpIndex.checkMeta(dump, index.getMetaFile(), index.getIndexType(), indexMeta);
         assertThat(indexMeta.getMetaValue("searchIndexVersion")).isEqualTo("1");
      }

      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {

         SearchIndex<Bean> index = with(dump, "_idLong", ( doc, o ) -> doc.add(new TextField("data", o._data, Store.NO))).withVersion(2).build();

         Bean firstBean = new Bean(1, "first row");
         dump.add(firstBean);
         index.flushMeta();

         IndexMeta indexMeta = new IndexMeta();
         DumpIndex.checkMeta(dump, index.getMetaFile(), index.getIndexType(), indexMeta);
         assertThat(indexMeta.getMetaValue("searchIndexVersion")).isEqualTo("2");
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
      long   _idLong;
      @externalize(2)
      int    _idInt;
      @externalize(3)
      String _idString;
      @externalize(10)
      String _data;

      public Bean() {
         // for Externalization
      }

      public Bean( int id, String data ) {
         _idLong = id;
         _idInt = id;
         _idString = (id < 0 ? "" : "+") + id;
         _data = data;
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

         if ( _idLong != bean._idLong ) {
            return false;
         }
         if ( _idInt != bean._idInt ) {
            return false;
         }
         if ( _idString != null ? !_idString.equals(bean._idString) : bean._idString != null ) {
            return false;
         }
         return _data != null ? _data.equals(bean._data) : bean._data == null;
      }

      @Override
      public int hashCode() {
         int result = (int)(_idLong ^ (_idLong >>> 32));
         result = 31 * result + _idInt;
         result = 31 * result + (_idString != null ? _idString.hashCode() : 0);
         result = 31 * result + (_data != null ? _data.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return "Bean{" + "_idLong=" + _idLong + ", _data='" + _data + '\'' + '}';
      }
   }
}
