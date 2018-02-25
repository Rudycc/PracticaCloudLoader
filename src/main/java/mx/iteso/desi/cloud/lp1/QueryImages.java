package mx.iteso.desi.cloud.lp1;

import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
import mx.iteso.desi.cloud.keyvalue.KeyValueStoreFactory;
import mx.iteso.desi.cloud.keyvalue.IKeyValueStorage;
import mx.iteso.desi.cloud.keyvalue.ParseTriples;
import mx.iteso.desi.cloud.keyvalue.PorterStemmer;

public class QueryImages {
  IKeyValueStorage imageStore;
  IKeyValueStorage titleStore;
	
  public QueryImages(IKeyValueStorage imageStore, IKeyValueStorage titleStore) 
  {
	  this.imageStore = imageStore;
	  this.titleStore = titleStore;
  }
	
  public Set<String> query(String word) {
    Set<String> articles;
    Set<String> images;
    Set<String> results = new HashSet<String>();
    String clean = PorterStemmer.stem(word);
    articles = titleStore.get(clean);

    Iterator<String> iter = articles.iterator();
    while (iter.hasNext()){
      images = imageStore.get(iter.next());
      Iterator<String> iter2 = images.iterator();
      while (iter2.hasNext()) {
        results.add(iter2.next());
      }
    }

    return results;
  }
        
  public void close() {
    imageStore.close();
    titleStore.close();
  }
	
  public static void main(String args[]) {
    System.out.println("*** Alumno: Rodolfo Carrillo Cuevas(Exp: Is700829 )");
    
    try{
      IKeyValueStorage imageStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
      "images");
      IKeyValueStorage titleStore = KeyValueStoreFactory.getNewKeyValueStore(Config.storeType, 
      "terms");
      
      QueryImages myQuery = new QueryImages(imageStore, titleStore);
  
      for (int i=0; i<args.length; i++) {
        System.out.println(args[i]+":");
        Set<String> result = myQuery.query(args[i]);
        Iterator<String> iter = result.iterator();
        while (iter.hasNext()) 
          System.out.println("  - "+iter.next());
      }
      
      myQuery.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Failed to complete the query -- exiting");
    }
  }
}

