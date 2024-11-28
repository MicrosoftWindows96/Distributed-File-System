import java.io.*;
import java.net.*;
import java.util.*;

public class Dstore {
  private static int p,cp,t;
  private static File ff;
  private static DSLogger dl;
  private static ServerSocket ds;
  private static Socket cs;

  public static void main(String[] a) throws IOException {
    if (a.length < 4) {
      System.out.println("Arg count error");
      return;
    }
    try {
      p = Integer.parseInt(a[0]);
      cp = Integer.parseInt(a[1]);
      t = Integer.parseInt(a[2]);
      ff = new File(a[3]);
    } catch (NumberFormatException e) {
      System.out.println("Arg error=" + e.getMessage());
      throw e;
    }
    try {
      DSLogger.init(Logger.LMode.CONSOLEFILE, p);
      dl = DSLogger.getInstance();
      System.out.println("Dstore Logger initialized");
      lg("Dstore: port=" + p + ", cport=" + cp + ", timeout=" + t + "ms, folder=" + ff);
    } catch (IOException e) {
      System.out.println("Failed to initialize Dstore Logger");
      throw e;
    }
    try {
      if (!ff.exists()) if (!ff.mkdir()) throw new IOException("Failed to create folder: " + ff.getAbsolutePath());
    } catch (IOException e) {
      lg("Folder open error");
      throw e;
    }
    try {
      cs = new Socket(InetAddress.getLoopbackAddress(), cp);
      j();
      lg("Join success");
    } catch (IOException e) {
      lg("Join failed");
      e.printStackTrace();
      throw e;
    }
    new Thread(() -> hr(cs)).start();
    try {
      ds = new ServerSocket(p);
      while (true) l();
    } catch (IOException e) {
      lg("Port bind error=" + p);
      e.printStackTrace();
      throw e;
    }
  }

  private static void l() {
    try {
      final Socket rs = ds.accept();
      new Thread(() -> hr(rs)).start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void j() throws IOException {
    tm(Protocol.JOIN_TOKEN, p, cs);
  }

  private static void hcm(Socket rs) throws IOException {
    BufferedReader cr = new BufferedReader(new InputStreamReader(rs.getInputStream()));
    String l;
    while ((l = cr.readLine()) != null) {
      dl.messageReceived(rs, l);
      Scanner s = new Scanner(l);
      String c = s.next();
      hc(c, s, rs);
    }
  }

  private static void hr(Socket rs) {
    try {
      hcm(rs);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lg("Closed: " + rs.getRemoteSocketAddress());
    }
  }

  private static void hc(String c, Scanner ps, Socket rs) throws IOException {
    rs.setSoTimeout(t);
    try {
      if (c.equals(Protocol.STORE_TOKEN)) {
        hS(rs, ps.next());
      } else if (c.equals(Protocol.LOAD_DATA_TOKEN)) {
        hL(rs, ps.next());
      } else if (c.equals(Protocol.REMOVE_TOKEN)) {
        hR(rs, ps.next());
      } else if (c.equals(Protocol.ACK_TOKEN)) {
        ap(rs);
      }
    } catch (NoSuchElementException e) {
      lg("Invalid request");
      e.printStackTrace();
    } finally {
      rs.setSoTimeout(0);
    }
  }

  private static void hS(Socket cs, String fn) throws IOException {
    tm(Protocol.ACK_TOKEN, cs);
    String fp = ff.getPath() + File.separator + fn;
    try (FileOutputStream os = new FileOutputStream(fp)) {
      lg("Storing: " + fn);
      cs.getInputStream().transferTo(os);
      lg("Stored: " + fn);
    }
    tm(Protocol.STORE_ACK_TOKEN, fn, Dstore.cs);
  }

  private static void ap(Socket ps) throws IOException {
    tm(Protocol.ACK_TOKEN, ps);
  }

  private static void hL(Socket cs, String fn) throws IOException {
    String fp = ff.getPath() + File.separator + fn;
    try (FileInputStream is = new FileInputStream(fp)) {
      lg("Sending: " + fn);
      is.transferTo(cs.getOutputStream());
      lg("Sent: " + fn);
    } catch (FileNotFoundException e) {
      tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, cs);
      cs.close();
    } finally {
      cs.close();
    }
  }

  private static void hR(Socket cs, String fn) throws IOException {
    File fd = new File(ff.getPath() + File.separator + fn);
    if (fd.exists()) {
      if (fd.delete()) {
        lg("Removed: " + fn);
        tm(Protocol.REMOVE_ACK_TOKEN, fn, cs);
      } else {
        lg("Remove failed: " + fn);
        tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, fn, cs);
      }
    } else {
      lg(fn + " not found");
      tm(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN, cs);
    }
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
    dl.messageSent(s, c + " " + d);
  }

  private static void tm(String c, Socket s) throws IOException {
    tm(c, null, s);
  }

  private static void lg(String m) {
    dl.logString(m);
  }
}