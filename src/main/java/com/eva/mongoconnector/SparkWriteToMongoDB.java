package com.eva.mongoconnector;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.spark.MongoSpark;
import com.td.mapper.ObfuscationConstants;
public class SparkWriteToMongoDB {

	private static JavaSparkContext jsc;
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
			      .master("local")
			      .appName("MongoSparkConnectorIntro")
			      .config("spark.mongodb.input.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
			      .config("spark.mongodb.output.uri", "mongodb://49.19.64.145/EDPP_OBFISCATION_DB.EDPP_OBFISCATION_COLLECTION")
			      .getOrCreate();

			     jsc = new JavaSparkContext(spark.sparkContext());

			    // Create a RDD of 10 documents
			    JavaRDD<Document> documents = jsc.parallelize(asList(110, 120, 130, 140, 150, 160, 170, 180, 190, 199)).map
			            (new Function<Integer, Document>() {
			      public Document call(final Integer i) throws Exception {
			          return Document.parse("{"+ObfuscationConstants.CUSTOMER_ID_JSON_TAG+": " + i + "}");
			      }
			    });

			    MongoSpark.save(parepareBulkInsert());
			    
			    /*Start Example: Save data from RDD to MongoDB*****************/
			    //MongoSpark.save(sparkDocuments, writeConfig);
			    //MongoSpark.save(documents);
			    /*End Example**************************************************/

			    jsc.close();

			  }
	private static JavaRDD<Document> parepareBulkInsert(){
		//DBCollection collection = db.getCollection("people");
		
		//DBObject document2 = new BasicDBObject();
		Document document1 = new Document();
		document1.put(ObfuscationConstants.CUSTOMER_ID_JSON_TAG, "420");
		document1.put(ObfuscationConstants.MASKED_CUSTMER_ID_JSON_TAG, "10420");

		//DBObject document2 = new BasicDBObject();
		Document document2 = new Document();
		document2.put(ObfuscationConstants.CUSTOMER_ID_JSON_TAG, "421");
		document2.put(ObfuscationConstants.MASKED_CUSTMER_ID_JSON_TAG, "10421");

		List<Document> documents = new ArrayList<>();
		documents.add(document1);
		documents.add(document2);
		//collection.insert(documents);
		return jsc.parallelize(documents);
	}
}
