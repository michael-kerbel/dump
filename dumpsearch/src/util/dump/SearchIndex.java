package util.dump;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
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
 * <p>For custom Analyzers (default is {@link StandardAnalyzer}) use the {@link IndexWriterConfig} constructor param. You have to use the same config
 * every time you use the index!</p>
 *
 * <p>For a custom QueryParser (default is {@link QueryParser}) use the constructor param. You have to use the same config
 * every time you use the index!</p>
 *
 * @see NumberQueryParser for a useful QueryParser to use when working with numbers
 */
public class SearchIndex<E> extends DumpIndex<E> {

   private IndexWriter             _writer;
   private QueryParser             _parser;
   private IndexSearcher           _searcher;
   private BiConsumer<Document, E> _documentBuilder;
   private IndexWriterConfig       _config;

   private AtomicBoolean _commitIsPending = new AtomicBoolean(false);


   public SearchIndex( @Nonnull Dump<E> dump, @Nonnull FieldAccessor idFieldAccessor, @Nonnull BiConsumer<Document, E> documentBuilder ) {
      this(dump, idFieldAccessor, documentBuilder, null, null);
   }

   public SearchIndex( @Nonnull Dump<E> dump, @Nonnull FieldAccessor idFieldAccessor, @Nonnull BiConsumer<Document, E> documentBuilder,
         @Nullable IndexWriterConfig config, @Nullable QueryParser queryParser ) {

      super(dump, idFieldAccessor, new File(dump.getDumpFile().getParentFile(), dump.getDumpFile().getName() + ".search.index"));

      _documentBuilder = documentBuilder;
      _config = config;
      if ( config == null ) {
         _config = new IndexWriterConfig(new StandardAnalyzer());
      }

      init();

      if ( queryParser == null ) {
         _parser = new QueryParser("id", _config.getAnalyzer());
         _parser.setDefaultOperator(Operator.AND);
      } else {
         _parser = queryParser;
         if ( config == null )
            _config = new IndexWriterConfig(_parser.getAnalyzer());
      }

      openSearcher();
   }

   public SearchIndex( @Nonnull Dump<E> dump, @Nonnull String idFieldName, @Nonnull BiConsumer<Document, E> documentBuilder ) throws NoSuchFieldException {
      this(dump, new FieldFieldAccessor(Reflection.getField(dump._beanClass, idFieldName)), documentBuilder);
   }

   @Override
   public void close() throws IOException {
      commit();

      if ( _writer != null )
         _writer.close();
      if ( _searcher != null && _searcher.getIndexReader() != null )
         _searcher.getIndexReader().close();

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
      return getSearcher().count(_parser.parse(query));
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
      ScoreDoc[] docs = getSearcher().search(_parser.parse(query), Math.max(1, maxHits)).scoreDocs;
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
   }

   protected void commit() throws IOException {
      if ( _commitIsPending.get() ) {
         _writer.commit();
         _commitIsPending.set(false);
      }
   }

   @Override
   protected String getIndexType() {
      return SearchIndex.class.getSimpleName();
   }

   protected IndexSearcher getSearcher() throws IOException {
      commit();
      DirectoryReader reader = DirectoryReader.openIfChanged((DirectoryReader)_searcher.getIndexReader(), _writer, false);
      if ( reader != null ) {
         _searcher = new IndexSearcher(reader);
      }
      return _searcher;
   }

   @Override
   protected void initLookupMap() {
      // does nothing
   }

   @Override
   protected void initLookupOutputStream() {
      try {
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
   }

   @Override
   void add( E o, long pos ) {
      try {
         Document doc = new Document();
         doc.add(new StringField("id", getId(o), Field.Store.YES));
         doc.add(new StringField("pos", "" + pos, Field.Store.YES));
         _documentBuilder.accept(doc, o);
         _writer.addDocument(doc);
         _commitIsPending.set(true);
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to add to index", e);
      }
   }

   @Override
   void delete( E o, long pos ) {
      try {
         _writer.deleteDocuments(_parser.parse("id:" + getId(o) + " pos:" + pos));
         _commitIsPending.set(true);
      }
      catch ( IOException | ParseException e ) {
         throw new RuntimeException("Failed to delete pos " + pos, e);
      }
   }

   @Override
   void update( long pos, E oldItem, E newItem ) {
      try {
         Document doc = new Document();
         doc.add(new StringField("id", getId(newItem), Field.Store.YES));
         doc.add(new StringField("pos", "" + pos, Field.Store.YES));
         _documentBuilder.accept(doc, newItem);
         _writer.updateDocument(new Term("pos", "" + pos), doc);
         _commitIsPending.set(true);
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to update index", e);
      }
   }

   private String getId( E o ) {
      return (_fieldIsInt ? getIntKey(o) : (_fieldIsLong ? getLongKey(o) : getObjectKey(o))).toString();
   }
}
