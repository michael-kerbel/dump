package util.dump;

import java.io.Closeable;
import java.util.Iterator;


public interface DumpIterator<E> extends Iterator<E>, Closeable {

   public long getPosition();
}
