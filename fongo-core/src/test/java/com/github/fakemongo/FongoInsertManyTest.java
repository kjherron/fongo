package com.github.fakemongo;

import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Behavior of real mongo driver's {@link com.mongodb.client.MongoCollection#insertMany} is as follows:
 * <p>
 * The first document of _id: 1 will insert successfully, but the second with the same _id insert will fail.
 * This will also stop additional documents left in the queue from being inserted.
 * <p>
 * With ordered set to false, the insert operation would continue with any remaining documents.
 *
 * @see <a href="https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/">db.collection.insertMany() &mdash; MongoDB Manual 3.4</a>
 */
public class FongoInsertManyTest {

  public static final boolean REAL_MONGO = false;

  @Rule
  public FongoRule fongoRule = new FongoRule(REAL_MONGO);

  @After
  public void tearDown() throws Exception {
    fongoRule.getDatabase("db").getCollection("collection").drop();
  }

  @Test
  public void should_insert_all_given_documents() throws Exception {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);

    // when
    collection.insertMany(asList(document1, document2, document3));

    // then
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    // Then
    assertThat(documents).containsExactly(document1, document2, document3);
  }

  @Test
  public void should_throw_MongoBulkWriteException_if_middle_document_is_duplicated_and_ordered() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document2Duplicate = new Document("_id", 2);
    Document document3 = new Document("_id", 3);

    InsertManyOptions options = new InsertManyOptions().ordered(true);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document2Duplicate, document3), options);
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(2), new BsonDocument(), 2)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(2);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document1, document2);
  }

  @Test
  public void should_throw_MongoBulkWriteException_if_middle_document_is_duplicated_and_unordered() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document2Duplicate = new Document("_id", 2);
    Document document3 = new Document("_id", 3);

    InsertManyOptions options = new InsertManyOptions().ordered(false);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document2Duplicate, document3), options);
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(2), new BsonDocument(), 2)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(3);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document1, document2, document3);
  }

  @Test
  public void should_throw_MongoBulkWriteException_if_multiple_duplicates_and_unordered() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document1Duplicate = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document2Duplicate = new Document("_id", 2);
    Document document3 = new Document("_id", 3);

    InsertManyOptions options = new InsertManyOptions().ordered(false);

    // When/Then
    try {
      collection.insertMany(asList(document1, document1Duplicate, document2, document2Duplicate, document3), options);
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(1), new BsonDocument(), 1),
          new BulkWriteError(11000, getDuplicateKeyMessage(2), new BsonDocument(), 3)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(3);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document1, document2, document3);
  }

  @Test
  public void should_throw_MongoBulkWriteException_on_first_insert_and_abort_consequent_inserts() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);
    collection.insertOne(document1);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document3));
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      // then
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(1), new BsonDocument(), 0)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(0);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document1);
  }

  @Test
  public void should_throw_MongoBulkWriteException_on_last_insert_and_have_previous_successful_inserts() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);
    collection.insertOne(document3);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document3));
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      // then
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(3), new BsonDocument(), 2)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(2);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document3, document1, document2);
  }

  @Test
  public void should_throw_MongoBulkWriteException_for_first_insert_and_complete_consequent_inserts_if_unordered() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);
    collection.insertOne(document1);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document3), new InsertManyOptions().ordered(false));
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(1), new BsonDocument(), 0)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(2);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document1, document2, document3);
  }

  @Test
  public void should_throw_MongoBulkWriteException_for_middle_insert_and_complete_other_inserts_if_unordered() {
    // Given
    MongoCollection<Document> collection = fongoRule.getDatabase("db").getCollection("collection");
    Document document1 = new Document("_id", 1);
    Document document2 = new Document("_id", 2);
    Document document3 = new Document("_id", 3);
    collection.insertOne(document2);

    // When/Then
    try {
      collection.insertMany(asList(document1, document2, document3), new InsertManyOptions().ordered(false));
      fail("expected exception was not thrown");
    } catch (MongoBulkWriteException ex) {
      assertThat(ex.getWriteErrors()).containsExactly(
          new BulkWriteError(11000, getDuplicateKeyMessage(2), new BsonDocument(), 1)
      );
      assertThat(ex.getWriteResult().getInsertedCount()).isEqualTo(2);
    }
    final List<Document> documents = collection.find().into(new ArrayList<Document>());

    assertThat(documents).containsExactly(document2, document1, document3);
  }

  private String getDuplicateKeyMessage(Object value) {
    if (REAL_MONGO) {
      return "E11000 duplicate key error index: db.collection.$_id_ dup key: { : " + value + " }";
    } else {
      return "E11000 duplicate key error index: db.collection._id  dup key : {[[" + value + "]] }";
    }
  }

}
