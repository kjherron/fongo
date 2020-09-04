package com.mongodb;


import com.github.fakemongo.Fongo;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.UUID;
import org.bson.Document;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * These tests check if a collection name is valid before being created by an insert command.
 * If it isn't valid an IllegalArgumentException will be thrown.
 * <p>
 * Reference: The _id field not returned for collections with name starting with "system" #242
 *
 * @author Nicola Viola
 */
public class CollectionNameTest {

  private static Fongo fongo;
  private UUID id;
  private Document document;

  @BeforeClass
  public static void setUpBeforeClass() {
    fongo = new Fongo("test");
  }

  @Before
  public void setUp() {
    id = UUID.randomUUID();
    document = new Document("_id", id);
  }

  @Test
  public void collectionnameIsSystemUser_thenTheDocumentIsFound() {

    String collectionName = "systemUser";
    UUID result = findADocumentInACollection(collectionName, document, fongo.getMongo());
    assertThat(result, is(id));
  }

  @Test(expected = IllegalArgumentException.class)
  public void collectionnameIsSystemDotUser_throwIllegalArgumentException() {
    String collectionName = "system.User";
    findADocumentInACollection(collectionName, document, fongo.getMongo());
  }

  @Test(expected = IllegalArgumentException.class)
  public void collectionnameIsSystemDot_throwIllegalArgumentException() {
    String collectionName = "system.";

    findADocumentInACollection(collectionName, document, fongo.getMongo());
  }

  @Test
  public void collectionnameIsSystem_thenTheDocumentIsFound() {
    String collectionName = "system";

    UUID result = findADocumentInACollection(collectionName, document, fongo.getMongo());
    assertThat(result, is(id));
  }


  @Test(expected = IllegalArgumentException.class)
  public void collectionnameIs$t_thenTheDocumentIsFound() {
    String collectionName = "$t";

    findADocumentInACollection(collectionName, document, fongo.getMongo());
  }


  private UUID findADocumentInACollection(String collectionName, Document document, MongoClient fongo) {

    MongoDatabase database = fongo.getDatabase("test-db");

    MongoCollection<Document> systemUsers = database.getCollection(collectionName);
    systemUsers.insertOne(document);
    final Document retrievedDocument = systemUsers.find().iterator().next();

    return retrievedDocument.get("_id", UUID.class);
  }


}