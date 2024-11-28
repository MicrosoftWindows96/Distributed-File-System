import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;

public class Controller {
  private static int cp, r, t, rp;
  private static final Set<CDSIn> sf = new HashSet<>();
  private static final Map<Integer, Socket> ds = new HashMap<>();
  private static final Map<Socket, List<Socket>> cld = new HashMap<>(), crd = new HashMap<>();
  private static CLogger cl;
  private static ServerSocket cs;

  public static void main(String[] a) {
    if (a.length < 4) {
      System.out.println("Insufficient arguments provided.");
      System.out.println("Expected: java Controller cport R timeout rebalancePeriod");
      return;
    }
    try {
      cp = Integer.parseInt(a[0]);
      r = Integer.parseInt(a[1]);
      t = Integer.parseInt(a[2]);
      rp = Integer.parseInt(a[3]);
    } catch (NumberFormatException e) {
      System.out.println("Unable to parse arguments: " + e.getMessage());
      System.out.println("All arguments must be integers. Expected format: java Controller cport R timeout rebalancePeriod");
      e.printStackTrace();
      return;
    }
    try {
      CLogger.init(Logger.LMode.CONSOLEFILE);
      System.out.println("The Controller's Logger has been initialised");
      cl = CLogger.getInstance();
      cl.logString("Controller initiated, Listening on port " + cp + "; Replication Factor: " + r + "; Timeout: " + t + "ms; Rebalance Period: " + rp + "ms");
    } catch (IOException e) {
      System.out.println("Unable to initialise the Controller's Logger");
      e.printStackTrace();
      return;
    }
    try {
      cs = new ServerSocket(cp);
      while (true) l();
    } catch (IOException e) {
      lg("Unable to bind to Controller Port: " + cp);
      e.printStackTrace();
    }
  }

  private static void l() {
    try {
      final Socket rs = cs.accept();
      new Thread(() -> hr(rs)).start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void hcm(Socket rs) {
    try {
      BufferedReader cr = new BufferedReader(new InputStreamReader(rs.getInputStream()));
      String l;
      while ((l = cr.readLine()) != null) {
        cl.messageReceived(rs, l);
        Scanner s = new Scanner(l);
        String c = s.next();
        hc(c, s, rs);
      }
    } catch (IOException e) {
      hdc(rs);
    }
  }

  private static void hdc(Socket s) {
    if (gdp(s) != -1) {
      for (CDSIn f : sf) {
        if (f.ds.contains(s)) {
          f.bds.add(s);
        }
        if (f.ds.isEmpty()) {
          sf.remove(f);
        }
      }
    }
  }

  private static void cc(Socket rs) {
    synchronized (ds) {
      rd(rs);
    }
    lg("Closed: " + rs.getRemoteSocketAddress());
  }

  private static void hr(Socket rs) {
    hcm(rs);
  }

  private static void hc(String c, Scanner ps, Socket rs) throws IOException {
    rs.setSoTimeout(t);
    try (ps) {
      if (c.equals(Protocol.JOIN_TOKEN)) {
        hJ(rs, ps.nextInt());
      } else if (c.equals(Protocol.LIST_TOKEN)) {
        hL(rs);
      } else if (c.equals(Protocol.STORE_TOKEN)) {
        hS(rs, ps.next(), ps.next());
      } else if (c.equals(Protocol.STORE_ACK_TOKEN)) {
        hSA(rs, ps.next());
      } else if (c.equals(Protocol.LOAD_TOKEN)) {
        hLO(rs, ps.next());
      } else if (c.equals(Protocol.RELOAD_TOKEN)) {
        hR(rs, ps.next());
      } else if (c.equals(Protocol.REMOVE_TOKEN)) {
        hRM(rs, ps.next());
      }
    } catch (NoSuchElementException e) {
      lg("Invalid request format");
      e.printStackTrace();
    } finally {
      rs.setSoTimeout(0);
    }
  }

  private static void hJ(Socket s, int p) {
    synchronized (ds) {
      ds.putIfAbsent(p, s);
    }
    cl.dstoreJoined(s, p);
    new Thread(() -> {
      try {
        BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));
        while (r.readLine() != null) {
        }
      } catch (IOException e) {
        synchronized (ds) {
          ds.remove(p);
        }
        lg("Dstore " + p + " disconnected");
      }
    }).start();
  }

  private static void hL(Socket cs) throws IOException {
    if (ds.size() >= r) {
      Set<String> fn = gfn(CDSIn.CDSMode.STORFIN);
      tm(Protocol.LIST_TOKEN, cts(fn), cs);
      lg("Sent file list: " + cts(fn));
    } else {
      tm(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, cs);
      lg("Insufficient Dstores to list files");
    }
  }

  private static boolean csf(Socket cs, String fn) throws IOException {
    if (ds.size() < r) {
      lg("Insufficient Dstores for " + fn);
      tm(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, cs);
      return false;
    } else if (gfn(CDSIn.CDSMode.values()).contains(fn)) {
      lg(fn + " exists");
      tm(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN, cs);
      return false;
    }
    return true;
  }

  private static void hS(Socket cs, String fn, String fs) throws IOException {
    synchronized (sf) {
      boolean fileExists = gfn(CDSIn.CDSMode.values()).contains(fn);
      if (ds.size() < r) {
        lg("Insufficient Dstores for " + fn);
        tm(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, cs);
        return;
      }
      CDSIn nf = new CDSIn(fn, fs, CDSIn.CDSMode.STOR, cs);
      sf.add(nf);

      List<Integer> ad = new ArrayList<>(ds.keySet());
      List<Integer> sd = ad.subList(0, Math.min(r, ad.size()));
      String sds = cts(sd);
      lg("Storing " + fn + " in " + sds);
      tm(Protocol.STORE_TO_TOKEN, sds, cs);

      if (fileExists) {
        lg(fn + " exists");
        tm(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN, cs);
        return;
      }

      new Thread(() -> {
        try {
          Thread.sleep(t);
          if (nf.a < r) {
            //sf.remove(nf);
            lg(fn + " store failed");
            try {
              tm(Protocol.ERROR_STORE_TOKEN, cs);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }).start();
    }
  }

  private static void ha(CDSIn f, String fn) throws IOException {
    synchronized (f.sl2) {
      int ac = ++f.a;
      lg(fn + " ack " + ac);
      if (f.a >= r) {
        f.s = CDSIn.CDSMode.STORFIN;
        lg(fn + " stored");
        tm(Protocol.STORE_COMPLETE_TOKEN, f.sl);
      }
    }
  }

  private static void hSA(Socket d, String fn) throws IOException {
    CDSIn f = gfbn(fn);
    if (f == null) return;
    f.ds.add(d);
    ha(f, fn);
  }

  private static List<Socket> fd(CDSIn df) {
    return df.ds.stream()
            .filter(ds -> !df.bds.contains(ds))
            .collect(Collectors.toList());
  }

  private static int gdp(Socket ds) {
    for (Map.Entry<Integer, Socket> e : Controller.ds.entrySet()) {
      if (e.getValue().equals(ds)) {
        return e.getKey();
      }
    }
    return -1;
  }

  private static void hLO(Socket cs, String fn) throws IOException {
    synchronized (sf) {
      if (ds.size() < r) {
        tm(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, cs);
        lg("Insufficient Dstores to load " + fn);
        return;
      }
      CDSIn df = gfbn(fn);
      if (df == null || df.s != CDSIn.CDSMode.STORFIN) {
        tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, cs);
        lg(fn + " not found");
        return;
      }
      synchronized (df) {
        List<Socket> ad = new ArrayList<>(df.ds);
        ad.removeAll(df.bds);
        List<Socket> td = cld.getOrDefault(cs, new ArrayList<>());
        ad.removeAll(td);
        if (!ad.isEmpty()) {
          Random r = new Random();
          int ri = r.nextInt(ad.size());
          Socket sd = ad.get(ri);
          td.add(sd);
          cld.put(cs, td);
          int dp = gdp(sd);
          if (dp != -1) {
            tm(Protocol.LOAD_FROM_TOKEN, dp + " " + df.fs, cs);
            return;
          } else {
            lg("Dstore port not found for " + sd);
          }
        }
      }
      tm(Protocol.ERROR_LOAD_TOKEN, cs);
      lg("No available Dstores to load " + fn);
      cld.remove(cs);
    }
  }

  private static void hR(Socket cs, String fn) throws IOException {
    CDSIn df = gfbn(fn);
    if (df == null || df.s != CDSIn.CDSMode.STORFIN) {
      tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, cs);
      lg(fn + " not found");
      return;
    }
    List<Socket> ad = new ArrayList<>(df.ds);
    ad.removeAll(df.bds);
    List<Socket> lld = cld.getOrDefault(cs, new ArrayList<>());
    if (!lld.isEmpty()) {
      ad.remove(lld.get(lld.size() - 1));
    }
    List<Socket> td = crd.getOrDefault(cs, new ArrayList<>());
    ad.removeAll(td);
    if (!ad.isEmpty()) {
      Random r = new Random();
      int ri = r.nextInt(ad.size());
      Socket sd = ad.get(ri);
      td.add(sd);
      crd.put(cs, td);
      int dp = gdp(sd);
      if (dp != -1) {
        tm(Protocol.LOAD_FROM_TOKEN, dp + " " + df.fs, cs);
        return;
      } else {
        lg("Dstore port not found for " + sd);
      }
    }
    tm(Protocol.ERROR_LOAD_TOKEN, cs);
    lg("No available Dstores to reload " + fn);
    crd.remove(cs);
  }

  private static void rd(Socket str) {
    ds.values().remove(str);
  }

  private static void hRM(Socket cs, String fn) throws IOException {
    synchronized (sf) {
      if (ds.size() < r) {
        tm(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN, cs);
        lg("Insufficient Dstores to remove " + fn);
        return;
      }
      CDSIn df = gfbn(fn);
      if (df == null || df.s != CDSIn.CDSMode.STORFIN) {
        tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, cs);
        lg(fn + " not found");
        return;
      }
      synchronized (df) {
        if (df.s == CDSIn.CDSMode.STORFIN && df.ds.size() - df.bds.size() >= r) {
          df.s = CDSIn.CDSMode.DEL;
          lg("Removing " + fn + " from " + df.ds);
          List<Thread> at = new ArrayList<>();
          for (Socket ds : df.ds) {
            if (!df.bds.contains(ds)) {
              int dp = gdp(ds);
              if (dp != -1) {
                try {
                  tm(Protocol.REMOVE_TOKEN, fn, ds);
                  lg("REMOVE sent to Dstore " + dp);
                  Thread act = new Thread(() -> {
                    try {
                      BufferedReader r = new BufferedReader(new InputStreamReader(ds.getInputStream()));
                      String rp = r.readLine();
                      if (rp != null) {
                        if (rp.startsWith(Protocol.REMOVE_ACK_TOKEN)) {
                          synchronized (df) {
                            df.ds.remove(ds);
                            if (df.ds.isEmpty()) {
                              sf.remove(df);
                            }
                          }
                          lg(fn + " removed");
                        } else if (rp.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) {
                          lg(fn + " not found");
                        } else {
                          lg("Unexpected response for " + fn + ": " + rp);
                        }
                      }
                    } catch (IOException e) {
                      lg("Error waiting for REMOVE_ACK from " + ds.getRemoteSocketAddress());
                      e.printStackTrace();
                    }
                  });
                  at.add(act);
                  act.start();
                } catch (IOException e) {
                  lg("Error sending REMOVE to " + ds.getRemoteSocketAddress());
                  e.printStackTrace();
                }
              } else {
                lg("Invalid Dstore: " + ds);
              }
            }
          }
          srt(df, cs);
          tm(Protocol.REMOVE_COMPLETE_TOKEN, fn, cs);
        } else {
          tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, cs);
          lg(fn + " not found or insufficient available Dstores");
        }
      }
    }
  }

  private static void srt(CDSIn df, Socket cs) {
    new Thread(() -> {
      try {
        Thread.sleep(t);
        synchronized (df.sl2) {
          if (df.s == CDSIn.CDSMode.DEL) {
            lg("Timeout removing " + df.fn);
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
  }


  private static List<Integer> gds() {
    return new ArrayList<>(ds.keySet());
  }

  private static void tm(String c, Object d, Socket s) throws IOException {
    PrintWriter sw = new PrintWriter(s.getOutputStream());
    sw.print(c);
    if (d != null) {
      sw.print(" ");
      sw.print(d);
    }
    sw.println();
    sw.flush();
    cl.messageSent(s, c + " " + d);
  }

  private static void tm(String c, Socket s) throws IOException {
    tm(c, null, s);
  }

  private static Set<String> gfn(CDSIn.CDSMode... ss) {
    return sf.stream()
            .filter(f -> Arrays.asList(ss).contains(f.s))
            .map(f -> f.fn)
            .collect(Collectors.toSet());
  }

  private static CDSIn gfbn(String fn) {
    return sf.stream()
            .filter(f -> f.fn.equals(fn))
            .findFirst()
            .orElse(null);
  }

  private static String cts(Collection<?> c) {
    return c.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(" "));
  }

  private static void lg(String m) {
    cl.logString(m);
  }
}