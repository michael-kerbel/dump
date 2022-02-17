package util.dump.sort;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import util.dump.DumpInput;


class ListInput<E> implements DumpInput<E> {

   int     _index = -1;
   List<E> _list;

   ListInput( List<E> list ) {
      _list = list;
   }

   public Iterator<E> iterator() {
      return _list.iterator();
   }

   public void close() throws IOException {}

}
