package util.dump;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import gnu.trove.list.TLongList;
import util.reflection.FieldAccessor;
import util.reflection.FieldFieldAccessor;
import util.reflection.Reflection;


/**
 * <p>A DumpIndex allowing the dump entries to be searched for using a Lucene index.</p>
 *
 * <p>You need to provide a documentBuilder, which adds the Lucene index fields you want to search later.</p>
 *
 * <p><code>
 *    SearchIndex<Bean> index = new SearchIndex<>(dump, new FieldFieldAccessor(field), ( doc, bean ) -> doc.add(new TextField("data", bean._data, Store.NO)));
 * </code></p>
 *
 * <p>When searching you need to prefix your query tokens with the field you want to search in. A classic {@link QueryParser} is being used by default,
 * with the internal "id" field and the AND operator as defaults.</p>
 *
 * <p><code>
 *    for(Bean bean: index.search("data:searchtoken")) doSomething(bean);
 * </code></p>
 *
 * <p>For custom Analyzers (default is {@link StandardAnalyzer}) use the {@link IndexWriterConfig} constructor param. See CustomAnalyzerTest for an example.
 * You have to use the same config every time you use the index!</p>
 *
 * <p>For a custom QueryParser (default is {@link QueryParser}) use the constructor param. You have to use the same config
 * every time you use the index!</p>
 *
 * @see NumberQueryParser for a useful QueryParser to use when working with numbers
 */
public class SearchIndex<E> extends DumpIndex<E> {

   public static <T> SearchIndexBuilder<T> with( @Nonnull Dump<T> dump, @Nonnull String idFieldName, @Nonnull BiConsumer<Document, T> documentBuilder )
         throws NoSuchFieldException {
      return new SearchIndexBuilder<>(dump, new FieldFieldAccessor(Reflection.getField(dump._beanClass, idFieldName)), documentBuilder);
   }

   public static <T> SearchIndexBuilder<T> with( @Nonnull Dump<T> dump, @Nonnull FieldAccessor idFieldAccessor,
         @Nonnull BiConsumer<Document, T> documentBuilder ) {
      return new SearchIndexBuilder<>(dump, idFieldAccessor, documentBuilder);
   }


   private IndexWriter             _writer;
   private QueryParser             _parser;
   private IndexSearcher           _searcher;
   private BiConsumer<Document, E> _documentBuilder;
   private IndexWriterConfig       _config;
   private AtomicBoolean           _commitIsPending = new AtomicBoolean(false);
   private DirectoryTaxonomyWriter _taxoWriter;
   private FacetsConfig            _facetsConfig;
   private DirectoryTaxonomyReader _taxoReader;


   private SearchIndex( @Nonnull Dump<E> dump, @Nonnull FieldAccessor idFieldAccessor, @Nonnull BiConsumer<Document, E> documentBuilder,
         @Nonnull IndexWriterConfig config, @Nonnull QueryParser queryParser, @Nonnull FacetsConfig facetsConfig ) {

      super(dump, idFieldAccessor, new File(dump.getDumpFile().getParentFile(), dump.getDumpFile().getName() + ".search.index"));

      _documentBuilder = documentBuilder;
      _config = config;
      _facetsConfig = facetsConfig;
      init();

      _parser = queryParser;

      openSearcher();
   }

   @Override
   public void close() throws IOException {
      commit();

      if ( _writer != null ) {
         _writer.close();
      }
      if ( _taxoWriter != null ) {
         _taxoWriter.close();
      }
      if ( _searcher != null && _searcher.getIndexReader() != null ) {
         _searcher.getIndexReader().close();
      }
      if ( _taxoReader != null ) {
         _taxoReader.close();
      }

      super.close();
   }

   /** Only provided to fulfill API requirements of parent class. You'll want to use {@link #countMatches(String)} or {@link #search(String)} most probably.
    * @return true if an entry exists in the dump whose id field has the value provided */
   @Override
   public boolean contains( int id ) {
      try {
         return search("id:" + id, 1).iterator().hasNext();
      }
      catch ( ParseException | IOException e ) {
         throw new RuntimeException("Failed to search", e);
      }
   }

   /** Only provided to fulfill API requirements of parent class. You'll want to use {@link #countMatches(String)} or {@link #search(String)} most probably.
    * @return true if an entry exists in the dump whose id field has the value provided */
   @Override
   public boolean contains( long id ) {
      try {
         return search("id:" + id, 1).iterator().hasNext();
      }
      catch ( ParseException | IOException e ) {
         throw new RuntimeException("Failed to search", e);
      }
   }

   /** Only provided to fulfill API requirements of parent class. You'll want to use {@link #countMatches(String)} or {@link #search(String)} most probably.
    * @return true if an entry exists in the dump whose id field has the value provided */
   @Override
   public boolean contains( Object id ) {
      try {
         return search("id:" + id.toString(), 1).iterator().hasNext();
      }
      catch ( ParseException | IOException e ) {
         throw new RuntimeException("Failed to search", e);
      }
   }

   /**
    * Counts the number of matches in the dump for the given Lucene query. This is way faster than searching and counting the results.
    * @param query a valid Lucene Query, which will be parsed using the provided Analyzer or StandardAnalyzer, if none provided.
    */
   public int countMatches( String query ) throws ParseException, IOException {
      return retryOnAlreadyClosed(() -> getSearcher().count(parse(query)));
   }

   public List<FacetResult> facetSearch( String query ) throws ParseException, IOException {
      return retryOnAlreadyClosed(() -> {
         FacetsCollector fc = new FacetsCollector();

         FacetsCollector.search(getSearcher(), parse(query), Integer.MAX_VALUE, fc);

         Facets facets = new FastTaxonomyFacetCounts(getTaxonomyReader(), _facetsConfig, fc);
         return facets.getAllDims(Integer.MAX_VALUE);
      });
   }

   @Override
   public void flush() throws IOException {
      super.flush();
      commit();
   }

   /**
    * Unimplemented!
    */
   @Override
   public TLongList getAllPositions() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getNumKeys() {
      try {
         return getSearcher().getIndexReader().numDocs();
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to query number of docs", e);
      }
   }

   public IndexSearcher getSearcher() throws IOException {
      commit();
      DirectoryReader reader = DirectoryReader.openIfChanged((DirectoryReader)_searcher.getIndexReader(), _writer, false);
      if ( reader != null ) {
         _searcher = new IndexSearcher(reader);
      }
      return _searcher;
   }

   public TaxonomyReader getTaxonomyReader() throws IOException {
      commit();
      DirectoryTaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(_taxoReader);
      if ( newTaxoReader != null ) {
         if ( _taxoReader != null ) {
            _taxoReader.close();
         }
         _taxoReader = newTaxoReader;
      }
      return _taxoReader;
   }

   /**
    * Searches for entries in the dump using the given Lucene query, loading all matching items lazily from the dump during iteration.
    * @param query a valid Lucene Query, which will be parsed using the provided Analyzer or StandardAnalyzer, if none provided.
    */
   public Iterable<E> search( String query ) throws ParseException, IOException {
      return search(query, getNumKeys());
   }

   /**
    * Searches for entries in the dump using the given Lucene query, loading all matching items lazily from the dump during iteration.
    * @param query a valid Lucene Query, which will be parsed using the provided Analyzer or StandardAnalyzer, if none provided.
    * @param maxHits the maximum number of results to return
    */
   public Iterable<E> search( String query, int maxHits ) throws ParseException, IOException {
      return retryOnAlreadyClosed(() -> {
         ScoreDoc[] docs = getSearcher().search(parse(query), Math.max(1, maxHits)).scoreDocs;
         return () -> new Iterator<E>() {

            int i = 0;


            @Override
            public boolean hasNext() {
               return i < docs.length;
            }

            @Override
            public E next() {
               try {
                  Document doc = getSearcher().doc(docs[i].doc);
                  String pos = doc.get("pos");
                  E e = _dump.get(Long.parseLong(pos));
                  i++;
                  return e;
               }
               catch ( IOException e ) {
                  throw new RuntimeException("Failed to perform search", e);
               }
            }
         };
      });
   }

   protected void commit() throws IOException {
      if ( _commitIsPending.get() ) {
         _writer.commit();
         _taxoWriter.commit();
         _commitIsPending.set(false);
      }
   }

   @Override
   protected String getIndexType() {
      return SearchIndex.class.getSimpleName();
   }

   @Override
   protected void initLookupMap() {
      // does nothing
   }

   @Override
   protected void initLookupOutputStream() {
      try {
         Directory facetDir = FSDirectory.open(new File(getLookupFile().getAbsolutePath() + "-facets").toPath());
         _taxoWriter = new DirectoryTaxonomyWriter(facetDir);
         _taxoWriter.commit();

         Directory dir = FSDirectory.open(getLookupFile().toPath());
         _config.setOpenMode(OpenMode.CREATE_OR_APPEND);
         _writer = new IndexWriter(dir, _config);
         _writer.commit();

      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to initialize dump index with lookup file " + getLookupFile(), e);
      }
   }

   @Override
   protected void load() {
      // does nothing, we only need to open the Searcher
   }

   protected void openSearcher() {
      try {
         _searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(getLookupFile().toPath())));
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to initialize dump index with lookup file " + getLookupFile(), e);
      }

      try {
         Directory facetDir = FSDirectory.open(new File(getLookupFile().getAbsolutePath() + "-facets").toPath());
         _taxoReader = new DirectoryTaxonomyReader(facetDir);
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to initialize dump facet index with lookup file " + getLookupFile() + "-facets", e);
      }
   }

   @Override
   void add( E o, long pos ) {
      try {
         Document doc = new Document();
         doc.add(new StringField("id", getId(o), Field.Store.YES));
         doc.add(new StringField("pos", "" + pos, Field.Store.YES));
         _documentBuilder.accept(doc, o);
         _writer.addDocument(_facetsConfig.build(_taxoWriter, doc));
         _commitIsPending.set(true);
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to add to index", e);
      }
   }

   @Override
   void delete( E o, long pos ) {
      try {
         _writer.deleteDocuments(parse("id:" + getId(o) + " pos:" + pos));
         _commitIsPending.set(true);
      }
      catch ( IOException | ParseException e ) {
         throw new RuntimeException("Failed to delete pos " + pos, e);
      }
   }

   synchronized Query parse( String query ) throws ParseException {
      return _parser.parse(query);
   }

   @Override
   void update( long pos, E oldItem, E newItem ) {
      try {
         Document doc = new Document();
         doc.add(new StringField("id", getId(newItem), Field.Store.YES));
         doc.add(new StringField("pos", "" + pos, Field.Store.YES));
         _documentBuilder.accept(doc, newItem);
         _writer.updateDocument(new Term("pos", "" + pos), _facetsConfig.build(_taxoWriter, doc));
         _commitIsPending.set(true);
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to update index", e);
      }
   }

   private String getId( E o ) {
      return (_fieldIsInt ? getIntKey(o) : (_fieldIsLong ? getLongKey(o) : getObjectKey(o))).toString();
   }

   /** We are optimistic with regard to concurrent modifications to the index. When it fails, we simply retry the underlying search. */
   private <T> T retryOnAlreadyClosed( ThrowingSupplier<T> s ) throws ParseException, IOException {
      AlreadyClosedException e = null;
      int n = 3;
      while ( n-- > 0 ) {
         try {
            return s.get();
         }
         catch ( AlreadyClosedException ee ) {
            // retry silently
            e = ee;
         }
      }
      throw new RuntimeException("Failed to execute search, even after multiple retries", e);
   }


   @FunctionalInterface
   public interface ThrowingSupplier<T> {

      /**
       * Gets a result.
       *
       * @return a result
       */
      T get() throws ParseException, IOException;
   }

   public static class SearchIndexBuilder<E> {

      private Dump<E>                 _dump;
      private BiConsumer<Document, E> _documentBuilder;
      FieldAccessor                   _idFieldAccessor;
      private Analyzer                _analyzer;
      private QueryParser             _queryParser;
      private FacetsConfig            _facetsConfig;
      private IndexWriterConfig       _indexWriterConfig;
      private String[]                _longFieldNames;
      private String[]                _doubleFieldNames;
      private String[]                _multiValuedFacetFields;


      public SearchIndexBuilder( Dump<E> dump, FieldAccessor idFieldAccessor, BiConsumer<Document, E> documentBuilder ) {
         _dump = dump;
         _documentBuilder = documentBuilder;
         _idFieldAccessor = idFieldAccessor;
      }

      @SuppressWarnings("unchecked")
      public <T> SearchIndex<T> build() {
         if ( _facetsConfig == null ) {
            _facetsConfig = new FacetsConfig();
         }
         if ( _multiValuedFacetFields != null ) {
            Arrays.stream(_multiValuedFacetFields).forEach(f -> _facetsConfig.setMultiValued(f, true));
         }
         if ( _analyzer == null ) {
            _analyzer = new StandardAnalyzer();
         }

         if ( _queryParser == null ) {
            _queryParser = new NumberQueryParser("id", _analyzer, _longFieldNames == null ? new String[0] : _longFieldNames,
               _doubleFieldNames == null ? new String[0] : _doubleFieldNames);
         } else if ( _longFieldNames != null || _doubleFieldNames != null ) {
            throw new RuntimeException("You may not set both a custom query parser and longFieldNames or doubleFieldNames");
         }

         if ( _indexWriterConfig == null ) {
            _indexWriterConfig = new IndexWriterConfig(_analyzer);
         }

         return new SearchIndex(_dump, _idFieldAccessor, _documentBuilder, _indexWriterConfig, _queryParser, _facetsConfig);
      }

      public SearchIndexBuilder<E> withAnalyzer( Analyzer analyzer ) {
         _analyzer = analyzer;
         return this;
      }

      public SearchIndexBuilder<E> withDoubleFields( String... doubleFieldNames ) {
         _doubleFieldNames = doubleFieldNames;
         return this;
      }

      public SearchIndexBuilder<E> withFacetsConfig( FacetsConfig facetsConfig ) {
         _facetsConfig = facetsConfig;
         return this;
      }

      public SearchIndexBuilder<E> withIndexWriterConfig( IndexWriterConfig indexWriterConfig ) {
         _indexWriterConfig = indexWriterConfig;
         return this;
      }

      public SearchIndexBuilder<E> withLongFields( String... longFieldNames ) {
         _longFieldNames = longFieldNames;
         return this;
      }

      public SearchIndexBuilder<E> withMultiValuedFacetFields( String... multiValuedFacetFields ) {
         _multiValuedFacetFields = multiValuedFacetFields;
         return this;
      }

      public SearchIndexBuilder<E> withQueryParser( QueryParser queryParser ) {
         _queryParser = queryParser;
         return this;
      }

   }

}
