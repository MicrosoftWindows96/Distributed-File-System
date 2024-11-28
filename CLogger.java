import java.io.IOException;
import java.net.Socket;

public class CLogger extends Logger {
  private static final String LFS = "Controller";
  private static CLogger i = null;

  public static void init(LMode lt) throws IOException {
    if (i == null) i = new CLogger(lt);
    else throw new IOException("CL init error: already initialised");
  }

  public static CLogger getInstance() {
    if (i == null) throw new RuntimeException("CL init error: not initialised yet");
    return i;
  }

  protected CLogger(LMode lt) throws IOException {
    super(lt);
  }

  @Override
  protected String getLFS() {
    return LFS;
  }

  public void dstoreJoined(Socket s, int dp) {
    logString("Dstore joined: " + dp + " (" + s.getLocalPort() + "<-" + s.getPort() + ")");
  }
}