import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class CDSIn {
  enum CDSMode {
    STOR,
    STORFIN,
    DEL,
    DELFIN
  }

  final String fn, fs;
  final List<Socket> ds, bds;
  final Socket sl;
  final Object sl2;
  CDSMode s;
  int a;
  final List<Socket> lc;

  CDSIn(String n, String sz, CDSMode st, Socket sb) {
    fn = n;
    fs = sz;
    ds = new ArrayList<>();
    bds = new ArrayList<>();
    s = st;
    a = 0;
    sl = sb;
    sl2 = new Object();
    lc = new ArrayList<>();
  }

  public void rd(Socket s) {
    ds.remove(s);
  }

  @Override
  public int hashCode() {
    return fn.hashCode();
  }

  @Override
  public String toString() {
    return fn;
  }
}