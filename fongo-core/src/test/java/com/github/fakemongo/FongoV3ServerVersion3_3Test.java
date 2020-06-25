package com.github.fakemongo;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.connection.ServerVersion;
import org.bson.Document;
import org.junit.Test;

public class FongoV3ServerVersion3_3Test extends AbstractFongoV3Test {

  @Override
  public ServerVersion serverVersion() {
    return Fongo.V3_3_SERVER_VERSION;
  }

    @Test
    public void insertOneWithDuplicateValueForUniqueColumn_throwsDuplicateKeyException() {
        // Given
        MongoCollection collection = newCollection();
        collection.createIndex(new Document("a", 1), new IndexOptions().name("a").unique(true));
        collection.insertOne(new Document("_id", 1).append("a", 1));
        collection.insertOne(new Document("_id", 2).append("a", 2));

        // When
        exception.expect(DuplicateKeyException.class);
        collection.findOneAndUpdate(docId(2), new Document("$set", new Document("a", 1)));
    }
}
