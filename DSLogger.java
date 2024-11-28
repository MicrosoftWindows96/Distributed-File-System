import java.io.IOException;

public class DSLogger extends Logger {
  private static final String LFS = "Dstore";
  private static DSLogger i = null;
  private final String lfs;

  public static void init(LMode lt, int p) throws IOException {
    if (i == null) i = new DSLogger(lt, p);
    else throw new IOException("DSL init error: already initialised");
  }

  public static DSLogger getInstance() {
    if (i == null) throw new RuntimeException("DSL init error: not initialised yet");
    return i;
  }

  protected DSLogger(LMode lt, int p) throws IOException {
    super(lt);
    lfs = LFS + "_" + p;
  }

  @Override
  protected String getLFS() {
    return lfs;
  }
}