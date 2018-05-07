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
         long lowerValue = part1 == null || "?".equals(part1) ? Long.MIN_VALUE : Long.parseLong(part1);
         long upperValue = part2 == null || "?".equals(part2) ? Long.MAX_VALUE : Long.parseLong(part2);
         return LongPoint.newRangeQuery(field, lowerValue, upperValue);
      }
      if ( _doubleFields.contains(field) ) {
         double lowerValue = part1 == null || "?".equals(part1) ? Double.MIN_VALUE : Double.parseDouble(part1);
         double upperValue = part2 == null || "?".equals(part2) ? Double.MAX_VALUE : Double.parseDouble(part2);
         return DoublePoint.newRangeQuery(field, lowerValue, upperValue);
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
