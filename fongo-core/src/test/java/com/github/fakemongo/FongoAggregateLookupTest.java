package com.github.fakemongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.springframework.data.mongodb.util.DBObjectUtils;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class FongoAggregateLookupTest {
  private static final String PARENT = "orders";
  private static final String CHILD = "orderItems";
  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  private ObjectId id1 = new ObjectId();
  private ObjectId id2 = new ObjectId();
  private ObjectId id3 = new ObjectId();
  private DBCollection parentCollection;
  private DBCollection childCollection;

  @Before
  public void setUp() {
    parentCollection = fongoRule.newCollection(PARENT);
    childCollection = fongoRule.newCollection(CHILD);

    childCollection.insert(new BasicDBObject("_id", id1).append("name", "Tom"));
    childCollection.insert(new BasicDBObject("_id", id2).append("name", "Jerry"));
    childCollection.insert(new BasicDBObject("_id", id3).append("name", "Itchy"));
  }

  @Test
  public void shouldJoinOnNonStringFields() {
    parentCollection.insert(new BasicDBObject("_id", "firstOrder").append("item", id1));
    parentCollection.insert(new BasicDBObject("_id", "newOrder").append("item", id2));
    parentCollection.insert(new BasicDBObject("_id", "disOrder").append("item", null));
    parentCollection.insert(new BasicDBObject("_id", "order66"));

    List<DBObject> result = lookupResults(createLookup("item", "_id", "lookedUpItem"));

    Assertions.assertThat(result).hasSize(4);
    assertContains(result, "firstOrder", 1, "Tom");
    assertContains(result, "newOrder", 1, "Jerry");
    assertContains(result, "disOrder", 0);
    assertContains(result, "order66", 0);
  }

  @Test
  public void shouldJoinOnCollectionFields() {
    parentCollection.insert(new BasicDBObject("_id", "firstOrder").append("item", DBObjectUtils.dbList(id1, id2)));
    parentCollection.insert(new BasicDBObject("_id", "camcOrder").append("item", id2));
    parentCollection.insert(new BasicDBObject("_id", "newOrder").append("item", id3));
    parentCollection.insert(new BasicDBObject("_id", "disOrder").append("item", null));
    parentCollection.insert(new BasicDBObject("_id", "order66"));
    
    List<DBObject> result = lookupResults(Arrays.asList(
            bo("$unwind", "$item"),
            createLookup("item", "_id", "lookedUpItem").get(0),
            bo("$unwind", "$lookedUpItem"),
            bo("$group", bo("_id", "$_id")
                    .append("item", bo("$push", "$item"))
                    .append("lookedUpItem", bo("$push", "$lookedUpItem")))));

    Assertions.assertThat(result).hasSize(3);
    assertContains(result, "firstOrder", 2, "Tom", "Jerry");
    assertContains(result, "camcOrder", 1, "Jerry");
    assertContains(result, "newOrder", 1, "Itchy");
  }

  @Test
  public void shouldJoinOnSameItem() {
    parentCollection.insert(new BasicDBObject("_id", "firstOrder").append("item", id1));
    parentCollection.insert(new BasicDBObject("_id", "newOrder").append("item", id1));

    List<DBObject> result = lookupResults(createLookup("item", "_id", "lookedUpItem"));
    
    Assertions.assertThat(result).hasSize(2);
    assertContains(result, "firstOrder", 1, "Tom");
    assertContains(result, "newOrder", 1, "Tom");
  }

  private List<DBObject> lookupResults(List<? extends DBObject> pipeline) {
    AggregationOutput output = parentCollection.aggregate(pipeline);

    List<DBObject> result = Lists.newArrayList(output.results());
    System.out.println(result);
    return result;
  }

  private void assertContains(List<DBObject> result, String _id, int matchedItemsSize, String... itemNames) {
    for (DBObject res : result) {
      if (res.get("_id").equals(_id)) {
        List<?> matchedItems = (List<?>) res.get("lookedUpItem");
        assertEquals(matchedItemsSize, matchedItems.size());
        if (matchedItemsSize > 0) {
          for (int i = 0; i < itemNames.length; ++i) {
            assertEquals(itemNames[i], ((DBObject) matchedItems.get(i)).get("name"));
          }
        }
        return;
      }
    }
    fail("Expected value from lookup collection not found. Wanted _id '" + _id + "' with item names: " + itemNames);
  }

  private List<? extends DBObject> createLookup(String localField, String foreignField, String as) {
    return Collections.singletonList(bo("$lookup", bo("from", CHILD)
        .append("localField", localField)
        .append("foreignField", foreignField)
        .append("as", as)));
  }

  private BasicDBObject bo(String key, Object val) {
    return new BasicDBObject(key, val);
  }

}
