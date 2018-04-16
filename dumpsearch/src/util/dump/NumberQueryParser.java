package util.dump;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;


/**
 * An extension to a classic {@link QueryParser} handling LongPoint and DoublePoint fields so that
 * efficient range and exact queries are created.
 */
public class NumberQueryParser extends QueryParser {

   private final Set<String> _longFields;
   private final Set<String> _doubleFields;


   public NumberQueryParser( String f, Analyzer a, String[] longFields, String[] doubleFields ) {
      super(f, a);
      _longFields = new HashSet<>(Arrays.asList(longFields));
      _doubleFields = new HashSet<>(Arrays.asList(doubleFields));
      setDefaultOperator(Operator.AND);
   }

   protected Query newRangeQuery( String field, String part1, String part2, boolean startInclusive, boolean endInclusive ) {

      if ( _longFields.contains(field) ) {
         return LongPoint.newRangeQuery(field, Long.parseLong(part1), Long.parseLong(part2));
      }
      if ( _doubleFields.contains(field) ) {
         return DoublePoint.newRangeQuery(field, Double.parseDouble(part1), Double.parseDouble(part2));
      }
      return super.newRangeQuery(field, part1, part2, startInclusive, endInclusive);
   }

   protected Query newTermQuery( Term term ) {
      if ( _longFields.contains(term.field()) ) {
         return LongPoint.newExactQuery(term.field(), Long.parseLong(term.text()));
      }
      if ( _doubleFields.contains(term.field()) ) {
         return DoublePoint.newExactQuery(term.field(), Double.parseDouble(term.text()));
      }
      return super.newTermQuery(term);

   }
}
