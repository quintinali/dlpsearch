package dlpsearch;



public class DlpEngine {
  private ESDriver es = null;
  
  public void end() {
    if (es != null) {
      es.close();
    }
  }


  public DlpEngine() {
    // TODO Auto-generated constructor stub
  }

  public static void main(String[] args) {
    DlpEngine me = new DlpEngine();
    me.es = new ESDriver();
    Importer imp = new Importer(me.es);
    imp.execute();
    
    
    me.end();
  }

}
