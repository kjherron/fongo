package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.FongoJSON;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class FongoAggregateGroupTest {

  @Rule
  public FongoRule fongoRule = new FongoRule(false);

  /**
   * See http://docs.mongodb.org/manual/reference/aggregation/concat/
   */
  @Test
  @Ignore("@twillouer : lots work to do.")
  public void testConcat() {
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ _id: 1, item: { sec: \"dessert\", category: \"pie\", type: \"apple\" } },\n" +
        "{ _id: 2, item: { sec: \"dessert\", category: \"pie\", type: \"cherry\" } },\n" +
        "{ _id: 3, item: { sec: \"main\", category: \"pie\", type: \"shepherd's\" } },\n" +
        "{ _id: 4, item: { sec: \"main\", category: \"pie\", type: \"chicken pot\" } }]");

    DBObject project = fongoRule.parseDBObject("{ $group: { _id:\n" +
        "                                    { $concat: [ \"$item.sec\",\n" +
        "                                                 \": \",\n" +
        "                                                 \"$item.category\"\n" +
        "                                               ]\n" +
        "                                    },\n" +
        "                               count: { $sum: 1 }\n" +
        "                             }\n" +
        "                   }");

    // When
    AggregationOutput output = coll.aggregate(project);

    // Then
    List<DBObject> resultAggregate = Lists.newArrayList(output.results());
    assertNotNull(resultAggregate);
    assertEquals(FongoJSON.parse("[\n" +
        "               { \"_id\" : \"main: pie\", \"count\" : 2 },\n" +
        "               { \"_id\" : \"dessert: pie\", \"count\" : 2 }\n" +
        "             ]"), resultAggregate);
  }

  @Test
  public void should_$avg_return_the_average_value() {
    // Given
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ \"_id\" : \"1\", \"test\" : 1.0, \"City\" : \"Madrid\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"2\", \"test\" : 10.0, \"City\" : \"Madrid\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"3\", \"test\" : 2.0, \"City\" : \"Madrid\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"4\", \"test\" : 20.0, \"City\" : \"Madrid\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"5\", \"test\" : 3.0, \"City\" : \"London\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"6\", \"test\" : 30.0, \"City\" : \"London\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"7\", \"test\" : 4.0, \"City\" : \"Paris\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"8\", \"test\" : 40.0, \"City\" : \"Paris\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"9\", \"test\" : 5.0, \"City\" : \"Paris\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"10\", \"test\" : 50.0, \"City\" : \"Paris\", \"groupType\" : \"treatment\"}]");

    // When
    AggregationOutput output = coll.aggregate(fongoRule.parseList("[{ \n" +
        "  \"$group\" : { \n" + //
        "      \"_id\" : { \"groupType\" : \"$groupType\" , \"City\" : \"$City\"} , \n" + //
        "      \"average\" : { \"$avg\" : \"$test\"}\n" +                                   //
        "  }\n" +                                                                             //
        "}]"));

    // Then
    Assertions.assertThat(output.results()).containsAll(fongoRule.parseList("[{\"_id\":{\"groupType\":\"treatment\", \"City\":\"Paris\"}, \"average\":45.0}, {\"_id\":{\"groupType\":\"treatment\", \"City\":\"London\"}, \"average\":30.0}, {\"_id\":{\"groupType\":\"control\", \"City\":\"London\"}, \"average\":3.0}, {\"_id\":{\"groupType\":\"control\", \"City\":\"Paris\"}, \"average\":4.5}, {\"_id\":{\"groupType\":\"treatment\", \"City\":\"Madrid\"}, \"average\":15.0}, {\"_id\":{\"groupType\":\"control\", \"City\":\"Madrid\"}, \"average\":1.5}]"));
  }

  @Test
  public void group_ShouldWorkWithAllEntries_IfUnwindOperationWasAppliedBeforehand() {
    // Given
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[" +
        "{ \"_id\" : \"1\", \"username\" : \"username1\", \"roles\" : [\"ROLE1\", \"ROLE2\"]},\n" +
        "{ \"_id\" : \"2\", \"username\" : \"username2\", \"roles\" : [\"ROLE1\", \"ROLE3\"]},\n" +
        "{ \"_id\" : \"3\", \"username\" : \"username3\", \"roles\" : [\"ROLE1\", \"ROLE4\"]},\n" +
        "{ \"_id\" : \"4\", \"username\" : \"username4\", \"roles\" : [\"ROLE2\", \"ROLE3\"]},\n" +
        "{ \"_id\" : \"5\", \"username\" : \"username5\", \"roles\" : [\"ROLE2\", \"ROLE4\", \"ROLE5\"]}" +
        "]");

    // When
    AggregationOutput output = coll.aggregate(fongoRule.parseList("[" +
        "{$unwind:\"$roles\"}, " +
        "{$group:{_id:\"$roles\", count:{$sum:1}, users:{$push:\"$username\"}}}" +
        "]"));

    // Then
    Assertions.assertThat(output.results()).containsAll(fongoRule.parseList("[" +
        "{\"_id\":\"ROLE1\",\"count\":3,\"users\":[\"username1\",\"username2\",\"username3\"]}," +
        "{\"_id\":\"ROLE2\",\"count\":3,\"users\":[\"username1\",\"username4\",\"username5\"]}," +
        "{\"_id\":\"ROLE3\",\"count\":2,\"users\":[\"username2\",\"username4\"]}," +
        "{\"_id\":\"ROLE4\",\"count\":2,\"users\":[\"username3\",\"username5\"]}," +
        "{\"_id\":\"ROLE5\",\"count\":1,\"users\":[\"username5\"]}" +
        "]"));
  }

  @Test
  public void should_$first_handle_$$ROOT_value() {
    // Given
    DBCollection coll = fongoRule.newCollection();
    fongoRule.insertJSON(coll, "[{ \"_id\" : \"1\", \"test\" : 1.0, \"City\" : \"Madrid\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"2\", \"test\" : 10.0, \"City\" : \"Madrid\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"3\", \"test\" : 2.0, \"City\" : \"Madrid\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"4\", \"test\" : 20.0, \"City\" : \"Madrid\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"5\", \"test\" : 3.0, \"City\" : \"London\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"6\", \"test\" : 30.0, \"City\" : \"London\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"7\", \"test\" : 4.0, \"City\" : \"Paris\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"8\", \"test\" : 40.0, \"City\" : \"Paris\", \"groupType\" : \"treatment\"},\n" +
        "{ \"_id\" : \"9\", \"test\" : 5.0, \"City\" : \"Paris\", \"groupType\" : \"control\"},\n" +
        "{ \"_id\" : \"10\", \"test\" : 50.0, \"City\" : \"Paris\", \"groupType\" : \"treatment\"}]");

    // When
    AggregationOutput output = coll.aggregate(fongoRule.parseList("[{ \n" +
        "  \"$group\" : { \n" + //
        "      \"_id\" : { \"groupType\" : \"$groupType\" , \"City\" : \"$City\"} , \n" + //
        "      \"data\" : { \"$push\" : \"$$ROOT\"}\n" +                                   //
        "  }\n" +                                                                             //
        "}]"));

    // Then
    Assertions.assertThat(output.results()).containsAll(fongoRule.parseList("[{\"_id\":{\"groupType\":\"treatment\", \"City\":\"Paris\"}, \"data\":[{\"_id\":\"8\", \"test\":40.0, \"City\":\"Paris\", \"groupType\":\"treatment\"}, {\"_id\":\"10\", \"test\":50.0, \"City\":\"Paris\", \"groupType\":\"treatment\"}]}, {\"_id\":{\"groupType\":\"control\", \"City\":\"Madrid\"}, \"data\":[{\"_id\":\"1\", \"test\":1.0, \"City\":\"Madrid\", \"groupType\":\"control\"}, {\"_id\":\"3\", \"test\":2.0, \"City\":\"Madrid\", \"groupType\":\"control\"}]}, {\"_id\":{\"groupType\":\"control\", \"City\":\"London\"}, \"data\":[{\"_id\":\"5\", \"test\":3.0, \"City\":\"London\", \"groupType\":\"control\"}]}, {\"_id\":{\"groupType\":\"treatment\", \"City\":\"London\"}, \"data\":[{\"_id\":\"6\", \"test\":30.0, \"City\":\"London\", \"groupType\":\"treatment\"}]}, {\"_id\":{\"groupType\":\"control\", \"City\":\"Paris\"}, \"data\":[{\"_id\":\"7\", \"test\":4.0, \"City\":\"Paris\", \"groupType\":\"control\"}, {\"_id\":\"9\", \"test\":5.0, \"City\":\"Paris\", \"groupType\":\"control\"}]}, {\"_id\":{\"groupType\":\"treatment\", \"City\":\"Madrid\"}, \"data\":[{\"_id\":\"2\", \"test\":10.0, \"City\":\"Madrid\", \"groupType\":\"treatment\"}, {\"_id\":\"4\", \"test\":20.0, \"City\":\"Madrid\", \"groupType\":\"treatment\"}]}]"));
  }
}
