package mx.iteso.desi.cloud.keyvalue;


import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import mx.iteso.desi.cloud.lp1.Config;

public class DynamoDBStorage extends BasicKeyValueStore {

    String dbName;

    // Simple autoincrement counter to make sure we have unique entries
    int inx;
    private AmazonDynamoDB client;
    private DynamoDB dynamoDB;

    Set<String> attributesToGet = new HashSet<String>();

    public DynamoDBStorage(String dbName) {
        this.dbName = dbName;
        this.client = AmazonDynamoDBClientBuilder.standard().withRegion(Config.amazonRegion).build();
        this.dynamoDB = new DynamoDB(this.client);
        init(dbName);
    }

    @Override
    public Set<String> get(String search) {
        Table table = dynamoDB.getTable(this.dbName);

        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#kw", "keyword");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":value", search);

        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#kw = :value").withNameMap(nameMap)
            .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;
        Set<String> result = new HashSet<String>();

        try {
            System.out.println("Items with " + search);
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                result.add(item.getString("value"));
                System.out.println(item.getString("value"));
            }
            return result.size() == 0 ? null : result;

        }
        catch (Exception e) {
            System.err.println("Unable to query table");
            System.err.println(e.getMessage());
        }
        return null;
    }

    @Override
    public boolean exists(String search) {
        return this.attributesToGet.contains(search);
    }

    @Override
    public Set<String> getPrefix(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addToSet(String keyword, String value) {
        put(keyword, value);
    }

    @Override
    public void put(String keyword, String value) {
        Table table = dynamoDB.getTable(this.dbName);
        try {
            System.out.println("Adding a new item..." + this.dbName);
            PutItemOutcome outcome = table
                .putItem(new Item().withPrimaryKey("keyword", keyword, "inx", this.inx++).withString("value", value));
            this.attributesToGet.add(keyword);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult() + " " + this.inx);
        }
        catch (Exception e) {
            System.err.println("Unable to add item: " + keyword + " " + value);
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void close() {
        dynamoDB.shutdown();
        client.shutdown();
    }
    
    @Override
    public boolean supportsPrefixes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean isCompressible() {
        return false;
    }

    @Override
    public boolean supportsMoreThan256Attributes() {
        return true;
    }

    public void init(String dbName){
        try {
            TableUtils.createTableIfNotExists(this.client, new CreateTableRequest(
                Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S),new AttributeDefinition("inx", ScalarAttributeType.N)),
                dbName,
                Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH),new KeySchemaElement("inx", KeyType.RANGE)),
                new ProvisionedThroughput(1L, 1L))
            );
            System.out.println("Creating table...");
            TableUtils.waitUntilExists(this.client, dbName);
            TableUtils.waitUntilActive(this.client, dbName);
            System.out.println("Table ready");
        } catch(Exception e) {
            System.err.println("Unable to access table");
            System.err.println(e.getMessage());
        }
    }
}
