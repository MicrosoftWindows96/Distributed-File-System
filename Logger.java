import java.io.*;
import java.net.Socket;

public abstract class Logger {
  public enum LMode {
    IGNORE,
    CONSOLE,
    FILE,
    CONSOLEFILE
  }

  protected final LMode lt;
  protected PrintStream ps;

  protected Logger(LMode lt) {
    this.lt = lt;
  }

  protected abstract String getLFS();

  protected synchronized PrintStream getPrintStream() throws IOException {
    if (ps == null)
      ps = new PrintStream(getLFS() + "_" + System.currentTimeMillis() + ".log");
    return ps;
  }

  protected boolean logToFile() {
    return lt == LMode.FILE || lt == LMode.CONSOLEFILE;
  }

  protected boolean logToConsole() {
    return lt == LMode.CONSOLE || lt == LMode.CONSOLEFILE;
  }

  protected void logString(String m) {
    if (logToFile())
      try { getPrintStream().println(m); } catch(Exception e) { e.printStackTrace(); }
    if (logToConsole())
      System.out.println(m);
  }

  public void messageSent(Socket s, String m) {
    logString("SENT [" + s.getLocalPort() + "->" + s.getPort() + "] " + m);
  }

  public void messageReceived(Socket s, String m) {
    logString("RECEIVED [" + s.getLocalPort() + "<-" + s.getPort() + "] " + m);
  }
}