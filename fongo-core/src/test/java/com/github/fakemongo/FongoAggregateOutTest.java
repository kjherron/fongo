package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import static com.github.fakemongo.junit.FongoRule.randomName;
import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import java.util.Arrays;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.util.Lists;
import org.bson.Document;
import static org.junit.Assert.assertEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;


/**
 * User: gdepourtales
 * 2015/06/15
 */
public class FongoAggregateOutTest {

  public final FongoRule fongoRule = new FongoRule(false);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule rules = RuleChain.outerRule(exception).around(fongoRule);

  /**
   * See http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out
   */
  @Test
  public void testOut() {
    DBCollection coll = fongoRule.newCollection();
    DBCollection secondCollection = fongoRule.newCollection();
    String data = "[{ _id: 1, sec: \"dessert\", category: \"pie\", type: \"apple\" },\n" +
        "{ _id: 2, sec: \"dessert\", category: \"pie\", type: \"cherry\" },\n" +
        "{ _id: 3, sec: \"main\", category: \"pie\", type: \"shepherd's\" },\n" +
        "{ _id: 4, sec: \"main\", category: \"pie\", type: \"chicken pot\" }]";
    fongoRule.insertJSON(coll, data);

    DBObject project = fongoRule.parseDBObject(
        "{ $out: \"" + secondCollection.getName() + "\"}"
    );

    AggregationOutput output = coll.aggregate(Arrays.asList(project));

    List<DBObject> resultAggregate = Lists.newArrayList(output.results());

    assertThat(resultAggregate).isEqualTo(fongoRule.parse(data));
    assertEquals(4, secondCollection.count());
    assertEquals("apple", secondCollection.find(fongoRule.parseDBObject("{_id:1}")).next().get("type"));
    assertEquals("pie", secondCollection.find(fongoRule.parseDBObject("{_id:1}")).next().get("category"));
    assertEquals("chicken pot", secondCollection.find(fongoRule.parseDBObject("{_id:4}")).next().get("type"));
  }


  /**
   * See http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out
   */
  @Test
  public void testOutWithEmptyCollection() {
    DBCollection coll = fongoRule.newCollection();
    DBCollection secondCollection = fongoRule.newCollection();
    String data = "[]";
    fongoRule.insertJSON(coll, data);

    DBObject project = fongoRule.parseDBObject(
        "{ $out: \"" + secondCollection.getName() + "\"}"
    );

    AggregationOutput output = coll.aggregate(Arrays.asList(project));
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());

    assertThat(resultAggregate).isEqualTo(fongoRule.parse(data));
    assertEquals(0, secondCollection.count());
  }

  /**
   * See http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out
   */
  @Test
  public void testOutWithNonExistentCollection() {
    DBCollection coll = fongoRule.newCollection();
    String data = "[{ _id: 1, sec: \"dessert\", category: \"pie\", type: \"apple\" },\n" +
        "{ _id: 2, sec: \"dessert\", category: \"pie\", type: \"cherry\" },\n" +
        "{ _id: 3, sec: \"main\", category: \"pie\", type: \"shepherd's\" } ,\n" +
        "{ _id: 4, sec: \"main\", category: \"pie\", type: \"chicken pot\" }]";
    fongoRule.insertJSON(coll, data);
    String newCollectionName = "new_collection";

    DBObject project = fongoRule.parseDBObject(
        "{ $out: \"" + newCollectionName + "\"}"
    );

    AggregationOutput output = coll.aggregate(Arrays.asList(project));
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());

    assertThat(resultAggregate).isEqualTo(fongoRule.parse(data));
    DBCollection newCollection = fongoRule.getDB().getCollection("new_collection");
    assertEquals(4, newCollection.count());
    assertEquals("apple", newCollection.find(fongoRule.parseDBObject("{_id:1}")).next().get("type"));
    assertEquals("pie", newCollection.find(fongoRule.parseDBObject("{_id:1}")).next().get("category"));
    assertEquals("chicken pot", newCollection.find(fongoRule.parseDBObject("{_id:4}")).next().get("type"));
  }

  /**
   * See http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out
   */
  @Test
  public void should_out_in_non_empty_collection() {
    MongoCollection coll = fongoRule.newMongoCollection(randomName("from"));
    MongoCollection secondCollection = fongoRule.newMongoCollection(randomName("out"));
    String data = "[{_id:3}, {_id:4}]";
    fongoRule.insertJSON(coll, data);
    fongoRule.insertJSON(secondCollection, "[{_id:1}, {_id:2}]");

    List<DBObject> aggregate = fongoRule.parseList("[{$project:{_id:1}}," +
        "{ $out: \"" + secondCollection.getNamespace().getCollectionName() + "\"}]"
    );

    AggregateIterable output = coll.aggregate(aggregate);
    List<DBObject> resultAggregate = Lists.newArrayList(output);
    System.out.println(resultAggregate);

    assertThat(resultAggregate).isEqualTo(Arrays.asList(new Document("_id", 3), new Document("_id", 4)));
    assertThat(secondCollection.find().iterator()).containsExactly(new Document("_id", 3), new Document("_id", 4));
  }
}
