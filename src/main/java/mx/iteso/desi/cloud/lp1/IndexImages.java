package mx.iteso.desi.cloud.lp1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;
import mx.iteso.desi.cloud.keyvalue.Triple;

public class IndexImages {
  ParseTriples parser;
  IKeyValueStorage imageStore, titleStore;
    
  public IndexImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
      
  public void run(String imageFileName, String titleFileName) throws IOException
  {
    // TODO: This method should load all images and titles 
    //       into the two key-value stores.
    parser = new ParseTriples(imageFileName);
    Triple data;
    while((data = parser.getNextTriple()) != null){
      if(data.getPredicate().equals("http://xmlns.com/foaf/0.1/depiction")){
        String[] file = data.getObject().split("FilePath/");
        if(file[1].startsWith(Config.filter)){
          imageStore.addToSet(data.getSubject(), data.getObject());
        }
      }
    }
    System.out.println("Finished parsing images");
    parser.close();

    parser = new ParseTriples(titleFileName);

    while((data = parser.getNextTriple()) != null){
      if(data.getPredicate().equals("http://www.w3.org/2000/01/rdf-schema#label") && imageStore.exists(data.getSubject())){
        String[] terms = data.getObject().split(" ");
        for(int i = 0; i < terms.length; i++){
          if(!PorterStemmer.stem(terms[i]).equals("Invalid term"))
            titleStore.addToSet(PorterStemmer.stem(terms[i]), data.getSubject());
          //System.out.println(terms[i]);
        }
      }
    }
    System.out.println("Finished parsing labels");
    parser.close();
  }
  
  public void close() {
    //TODO: close the databases;
    imageStore.close();
    titleStore.close();
  }
  
  public static void main(String args[])
  {
    // TODO: Add your own name here
    System.out.println("*** Alumno: Rodolfo Carrillo Cuevas (Exp: Is700829 )");
    try {

      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
    			"images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
  			"terms");


      IndexImages indexer = new IndexImages(imageStore, titleStore);
      indexer.run(Config.imageFileName, Config.titleFileName);
      System.out.println("Indexing completed");
      indexer.close();
      
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the indexing pass -- exiting");
    }
  }
}

