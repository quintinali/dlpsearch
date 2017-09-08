package dlpsearch;

import java.io.IOException;

import org.elasticsearch.action.index.IndexRequest;
import org.apache.commons.io.IOUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.*;

public class Importer {
  private ESDriver es = null;


  public Importer(ESDriver es) {
    this.es = es;
  }
  
  public void execute() {
    es.createBulkProcessor();
    System.out.println("Start metadata ingesting.");
    importToES();
    System.out.println("Metadata ingesting is finished");
    es.destroyBulkProcessor();
    es.refreshIndex();
  }
  
  private void importToES() {
    File directory = new File(SysConstants.METADATA_PATH);
    if(!directory.exists())
      directory.mkdir();
    File[] fList = directory.listFiles();
    for (File file : fList) {
      InputStream is;
      try {
        is = new FileInputStream(file);
        importSingleFileToES(is);
      } catch (FileNotFoundException e) {
      }

    }
  }
  
  private void importSingleFileToES(InputStream is) {
    try {
      String jsonTxt = IOUtils.toString(is);
      JsonParser parser = new JsonParser();
      JsonElement item = parser.parse(jsonTxt);
      IndexRequest ir = new IndexRequest(SysConstants.ES_INDEX_NAME, SysConstants.METADATA_TYPE).source(item.toString());
      es.getBulkProcessor().add(ir);
    } catch (IOException e) {
    }
  }

}
