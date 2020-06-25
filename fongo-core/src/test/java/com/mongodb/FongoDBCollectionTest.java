package com.mongodb;

import static com.github.fakemongo.impl.Util.list;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Iterables;
import org.assertj.core.api.Assertions;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.github.fakemongo.Fongo;

public class FongoDBCollectionTest {

  private static final String ARBITRARY_ID = UUID.randomUUID().toString();

  private FongoDBCollection collection;

  @Before
  public void setUp() {
    collection = (FongoDBCollection) new Fongo("test").getDB("test").getCollection("test");
  }

  @Test
  public void applyProjectionsInclusionsOnly() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 1));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("b", "b");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyElemMatchProjectionsInclusionsOnly() {
    BasicDBList dbl = new BasicDBList();
    dbl.add(new BasicDBObject("a","a"));
    dbl.add(new BasicDBObject("b","b"));
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", dbl);
    DBObject actual = collection.applyProjections(obj,
        new BasicDBObject().append("list", new BasicDBObject("$elemMatch", new BasicDBObject("b", "b"))));
    BasicDBList expextedDbl = new BasicDBList();
    expextedDbl.add(new BasicDBObject("b","b"));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", expextedDbl);
    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyElemMatchProjectionsMultiFieldInclusionsOnly() {
    BasicDBList dbl = new BasicDBList();
    dbl.add(new BasicDBObject("a","a").append("b", "b"));
    dbl.add(new BasicDBObject("c","c").append("d", "d"));
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", dbl);
    DBObject actual = collection.applyProjections(obj,
        new BasicDBObject().append("list", new BasicDBObject("$elemMatch", new BasicDBObject("d", "d"))));
    BasicDBList expextedDbl = new BasicDBList();
    expextedDbl.add(new BasicDBObject("c","c").append("d", "d"));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", expextedDbl);
    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyElemMatchProjectionsMultiFieldWithIdInclusionsOnly() {
    BasicDBList dbl = new BasicDBList();
    dbl.add(new BasicDBObject("c","a").append("_id", "531ef0060bd4d252a197bdaf"));
    dbl.add(new BasicDBObject("c","c").append("_id", "531ef0060bd4d252a197bda7"));
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", dbl);
    DBObject actual = collection.applyProjections(obj,
        new BasicDBObject().append("list", new BasicDBObject("$elemMatch",
                new BasicDBObject("_id", new ObjectId("531ef0060bd4d252a197bda7")))));
    BasicDBList expextedDbl = new BasicDBList();
    expextedDbl.add(new BasicDBObject("c","c").append("_id", "531ef0060bd4d252a197bda7"));
    DBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("list", expextedDbl);
    assertEquals("applied", expected, actual);
  }

  /** Tests multiprojections that are nested with the same prefix: a.b.c and a.b.d */
  @Test
  public void applyNestedProjectionsInclusionsOnly() {
      final DBObject obj = new BasicDBObjectBuilder()
          .add("_id", ARBITRARY_ID)
          .add("foo", 123)
          .push("a")
              .push("b")
                  .append("c", 50)
                  .append("d", 1000)
                  .append("bar", 1000)
      .get();

    final DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("a.b.c", 1)
                                                                                 .append("a.b.d", 1));
    final DBObject expected =  new BasicDBObjectBuilder()
                        .add("_id", ARBITRARY_ID)
                        .push("a")
                            .push("b")
                                .append("c", 50)
                                .append("d", 1000)
                           .get();

    assertEquals("applied", expected, actual);
  }


  @Test
  public void applyProjectionsInclusionsWithoutId() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 1));
    BasicDBObject expected = new BasicDBObject().append("b", "b");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectionsExclusionsOnly() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b", 0));
    BasicDBObject expected = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a");

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectsNestedExclusionsOnly() {
    BasicDBObject obj = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", new BasicDBObject()
            .append("c", "c")
            .append("d", "d"));
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("b.c", 0));
    BasicDBObject expected = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", new BasicDBObject()
            .append("d", "d"));

    assertEquals("applied", expected, actual);
  }

  @Test
  public void applyProjectionsExclusionsWithoutId() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("_id", 0).append("b", 0));
    BasicDBObject expected = new BasicDBObject().append("a", "a");

    assertEquals("applied", expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void applyProjectionsInclusionsAndExclusionsMixedThrowsException() {
    BasicDBObject obj = new BasicDBObject().append("_id", ARBITRARY_ID).append("a", "a").append("b", "b");
    collection.applyProjections(obj, new BasicDBObject().append("a", 1).append("b", 0));
  }


  @Test
  public void applyNestedArrayFieldProjection() {
    BasicDBObject obj = new BasicDBObject("_id", 1).append("name","foo")
      .append("seq", Arrays.asList(new BasicDBObject("a", "b"), new BasicDBObject("c", "b")));
    collection.insert(obj);

    List<DBObject> results = collection.find(new BasicDBObject("_id", 1),
        new BasicDBObject("_id", -1).append("seq.a", 1)).toArray();

    BasicDBObject expectedResult = new BasicDBObject("seq",
      Arrays.asList(new BasicDBObject("a", "b"), new BasicDBObject()));

    assertEquals("should have projected result", Arrays.asList(expectedResult), results);
  }

  @Test
  public void applyNestedFieldProjection() {

    collection.insert(new BasicDBObject("_id", 1)
      .append("a",new BasicDBObject("b", new BasicDBObject("c", 1))));

    collection.insert(new BasicDBObject("_id", 2)
      .append("a",new BasicDBObject("b", 1)));

    collection.insert(new BasicDBObject("_id", 3));

    List<DBObject> results = collection.find(new BasicDBObject(),
        new BasicDBObject("_id", -1).append("a.b.c", 1)).toArray();

    assertEquals("should have projected result", Arrays.asList(
      new BasicDBObject("a",new BasicDBObject("b", new BasicDBObject("c", 1))),
      new BasicDBObject("a",new BasicDBObject()),
      new BasicDBObject()
    ), results);
  }

  @Test
  public void applyDeeplyNestedProjectionExclusion() {
    BasicDBObject obj = new BasicDBObject()
        .append("_id", 1)
        .append("foo", "bar")
        .append("level1",
            new BasicDBObject("level2", list(
                new BasicDBObject()
                    .append("foo2", "bar2")
                    .append("level3", list(
                        new BasicDBObject("level4", "bob")
                    )))));
    collection.insert(obj);

    BasicDBObject query = new BasicDBObject("_id", 1);
    DBObject full = collection.findOne(query);
    assertEquals(obj, full);

    List<DBObject> fullAsList = collection.find(query).toArray();
    assertEquals(obj, Iterables.getOnlyElement(fullAsList));

    BasicDBObject projection = new BasicDBObject("level1.level2.level3", false);
    DBObject projected = collection.findOne(query, projection);
    BasicDBObject expected = new BasicDBObject()
        .append("_id", 1)
        .append("foo", "bar")
        .append("level1",
            new BasicDBObject("level2", list(
                new BasicDBObject()
                    .append("foo2", "bar2")
            )));
    assertEquals(expected, projected);
  }

  @Test
  public void findByListInQuery(){
    BasicDBObject existing = new BasicDBObject().append("_id", 1).append("aList", asDbList("a", "b", "c"));
    collection.insert(existing);
    DBObject result = collection.findOne(existing);
    assertEquals("should have projected result", existing, result);
  }

  BasicDBList asDbList(Object ... objects) {
     BasicDBList list = new BasicDBList();
     list.addAll(Arrays.asList(objects));
     return list;
  }

  @Test
  public void findByMapInListInQueryWithAllNestedElemMatch() {
    BasicDBObject existing = new BasicDBObject()
        .append("_id", 1)
        .append("aList", asDbList(
            new BasicDBObject().append("key", "a").append("value", 1),
            new BasicDBObject().append("key", "b").append("value", 2),
            new BasicDBObject().append("key", "c").append("value", 3)
        ));
    collection.insert(existing);
    DBObject query =
        new BasicDBObject("aList",
            new BasicDBObject("$all", asDbList(
                new BasicDBObject(
                    "$elemMatch",
                    new BasicDBObject().append("key", "a").append("value", 1)),
                new BasicDBObject(
                    "$elemMatch",
                    new BasicDBObject().append("key", "b").append("value", 2))
            ))
        );
    DBObject result = collection.findOne(query);
    assertEquals("should have found result", existing, result);
  }

  /** Tests multiprojections that are nested with the same prefix: a.b.c and a.b.d */
  @Test
  public void applyProjectionsWithBooleanValues() {
     final DBObject obj = new BasicDBObjectBuilder()
          .add("_id", ARBITRARY_ID)
          .add("foo", "oof")
          .add("bar", "rab")
          .add("gone", "fishing")
      .get();

    final DBObject actual = collection.applyProjections(obj, new BasicDBObject().append("foo", true)
                                                                                 .append("bar", 1));
    final DBObject expected =  new BasicDBObjectBuilder()
                        .add("_id", ARBITRARY_ID)
                        .add("foo", "oof")
                        .add("bar", "rab")
                        .get();

    assertEquals("applied", expected, actual);
  }


  @Test
  public void filterListDoentModifyEntry() {
    DBObject object = new BasicDBObject() {
        @Override
        public Object put(String key, Object val) {
            throw new IllegalStateException();
        }

        @Override
        public void putAll(Map m) {
            throw new IllegalStateException();
        }

        @Override
        public void putAll(BSONObject o) {
            throw new IllegalStateException();
        }

        @Override
        public BasicDBObject append(String key, Object val) {
            throw new IllegalStateException();
        }
    };

    DBObject r = collection.filterLists(object);
    assertTrue(r != object);
  }

  @Test
  public void setResultObjectClassForFindAndModify() {
    final BasicDBObject object = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", "b");
    collection.insert(object);
    collection.setObjectClass(TestResultObject.class);

    final TestResultObject result = (TestResultObject) collection.findAndModify(object, new BasicDBObject());
    final String id = result.getEntityId();
    assertThat(id).isEqualTo(ARBITRARY_ID);
  }

  @Test
  public void setResultObjectClassForFindOne() {
    final BasicDBObject object = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", "b");
    collection.insert(object);
    collection.setObjectClass(TestResultObject.class);

    final TestResultObject result = (TestResultObject) collection.findOne(object, new BasicDBObject());
    final String id = result.getEntityId();
    assertThat(id).isEqualTo(ARBITRARY_ID);
  }

  @Test
  public void setResultObjectClassFind() {
    final BasicDBObject object = new BasicDBObject()
        .append("_id", ARBITRARY_ID)
        .append("a", "a")
        .append("b", "b");
    collection.insert(object);
    collection.setObjectClass(TestResultObject.class);

    final DBCursor cursorWithKeys = collection.find(object, new BasicDBObject());
    final TestResultObject resultWithKeys = (TestResultObject) cursorWithKeys.next();
    assertThat(resultWithKeys.getEntityId()).isEqualTo(ARBITRARY_ID);

    final DBCursor cursorWithQuery = collection.find(object);
    final TestResultObject resultWithQuery = (TestResultObject) cursorWithQuery.next();
    assertThat(resultWithQuery.getEntityId()).isEqualTo(ARBITRARY_ID);

    final DBCursor cursor = collection.find();
    final TestResultObject result = (TestResultObject) cursor.next();
    assertThat(result.getEntityId()).isEqualTo(ARBITRARY_ID);
  }

  @Test
  public void sparseNonUniqueIndex() {
    collection.createIndex(new BasicDBObject("sparseNonUnique", 1), new BasicDBObject("sparse", true));
    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("sparseNonUnique", "42").append("other", "value1");
    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("other", "value2");
    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("other", "value3");
    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id4")
            .append("sparseNonUnique", "42").append("other", "value4");
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    collection.insert(obj4);
    DBCursor result = collection.find(new BasicDBObject("sparseNonUnique", "42"));
    Assertions.assertThat(result.size()).isEqualTo(2);
    DBCursor result3 = collection.find(new BasicDBObject("other", new BasicDBObject("$exists", true)));
    Assertions.assertThat(result3.size()).isEqualTo(4);
  }

  @Test
  public void sparseUniqueIndexSubList() {
    collection.createIndex(new BasicDBObject("sparseUnique.sublist", 1),
            new BasicDBObject().append("unique", true).append("sparse", true)
                .append("name", "sparseUnique.sublist"));

    BasicDBList list1 = new BasicDBList();
    list1.add(new BasicDBObject("sublist","42"));
    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("sparseUnique", list1);

    BasicDBList list2 = new BasicDBList();
    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("sparseUnique", list2);

    BasicDBList list3 = new BasicDBList();
    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("sparseUnique", list3);

    BasicDBList list4 = new BasicDBList();
    list4.add(new BasicDBObject("sublist","42"));
    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id1")
            .append("sparseUnique", list4);
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    boolean sawException = false;
    try {
      collection.insert(obj4);
    } catch (DuplicateKeyException e) {
      sawException = true;
    }
    DBCursor result = collection.find(new BasicDBObject("sparseUnique.sublist", "42"));
    Assertions.assertThat(result.size()).isEqualTo(1);
    Assertions.assertThat(sawException).isEqualTo(true);
    DBCursor result2 = collection.find(new BasicDBObject("sparseUnique", new BasicDBObject("$size", 0)));
    Assertions.assertThat(result2.size()).isEqualTo(2);
    DBCursor result3 = collection.find(new BasicDBObject("sparseUnique", new BasicDBObject("$exists", true)));
    Assertions.assertThat(result3.size()).isEqualTo(3);
  }

  @Test
  public void sparseUniqueIndexSubdoc() {
    collection.createIndex(new BasicDBObject("sparseUnique.subdoc", 1),
            new BasicDBObject().append("unique", true).append("sparse", true)
                .append("name", "sparseUnique.subdoc"));

    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("sparseUnique", new BasicDBObject("subdoc","42"));

    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("sparseUnique", new BasicDBObject());

    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("sparseUnique", new BasicDBObject());

    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id1")
            .append("sparseUnique", new BasicDBObject("subdoc","42"));
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    boolean sawException = false;
    try {
      collection.insert(obj4);
    } catch (DuplicateKeyException e) {
      sawException = true;
    }
    DBCursor result = collection.find(new BasicDBObject("sparseUnique.subdoc", "42"));
    Assertions.assertThat(result.size()).isEqualTo(1);
    Assertions.assertThat(sawException).isEqualTo(true);
    DBCursor result3 = collection.find(new BasicDBObject("sparseUnique", new BasicDBObject("$exists", true)));
    Assertions.assertThat(result3.size()).isEqualTo(3);
  }

  @Test
  public void textSearch() {
    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("textField", "tomorrow, and tomorrow, and tomorrow, creeps in this petty pace");
    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("textField", "eee, abc def");
    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("textField", "bbb, ccc");
    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id4")
            .append("textField", "aaa, bbb");
    BasicDBObject obj5 = new BasicDBObject().append("_id", "_id5")
            .append("textField", "bbb, fff");
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    collection.insert(obj4);
    collection.insert(obj5);
    collection.createIndex(new BasicDBObject("textField", "text"));
    DBObject actual = collection.text("aaa bbb -ccc -ddd -яяя \"abc def\" \"def bca\"", 0, new BasicDBObject());

    BasicDBList resultsExpected = new BasicDBList();
      resultsExpected.add(new BasicDBObject("score", 1.5)
              .append("obj", new BasicDBObject("_id", "_id2").append("textField", "eee, abc def")));
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id4").append("textField", "aaa, bbb")));
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id5").append("textField", "bbb, fff")));
    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", resultsExpected);
    expected.put("stats",
            new BasicDBObject("nscannedObjects", 6L)
            .append("nscanned", 5L)
            .append("n", 3L)
            .append("timeMicros", 1));
    expected.put("ok", 1);
    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void textSearchExactMatch() {
    BasicDBObject obj1 = new BasicDBObject().append("_id", "_id1")
            .append("textField", "tomorrow, and tomorrow, and tomorrow, creeps in this petty pace");
    BasicDBObject obj2 = new BasicDBObject().append("_id", "_id2")
            .append("textField", "eee, abc def");
    BasicDBObject obj3 = new BasicDBObject().append("_id", "_id3")
            .append("textField", "bbb, ccc");
    BasicDBObject obj4 = new BasicDBObject().append("_id", "_id4")
            .append("textField", "aaa, bbb");
    BasicDBObject obj5 = new BasicDBObject().append("_id", "_id5")
            .append("textField", "bbb, fff");
    BasicDBObject obj6 = new BasicDBObject().append("_id", "_id6")
            .append("textField", "aaa aaa eee, abc def");
    BasicDBObject obj7 = new BasicDBObject().append("_id", "_id7")
            .append("textField", "aaaaaaa");
    BasicDBObject obj8 = new BasicDBObject().append("_id", "_id8")
            .append("textField", "aaaaaaaa");
    collection.insert(obj1);
    collection.insert(obj2);
    collection.insert(obj3);
    collection.insert(obj4);
    collection.insert(obj5);
    collection.insert(obj6);
    collection.insert(obj8);
    collection.createIndex(new BasicDBObject("textField", "text"));
    DBObject actual = collection.text("aaa", 0, new BasicDBObject("textField", 1));

    BasicDBList resultsExpected = new BasicDBList();
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id4").append("textField", "aaa, bbb")));
      resultsExpected.add(new BasicDBObject("score", 0.75)
              .append("obj", new BasicDBObject("_id", "_id6").append("textField", "aaa aaa eee, abc def")));
    DBObject expected = new BasicDBObject("language", "english");
    expected.put("results", resultsExpected);
    expected.put("stats",
            new BasicDBObject("nscannedObjects", 2L)
            .append("nscanned", 2L)
            .append("n", 2L)
            .append("timeMicros", 1));
    expected.put("ok", 1);
    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testCount(){
    long alphabetCnt = 0;
    for (char a = 'A'; a <= 'Z'; a++, alphabetCnt++){
    	collection.insert(new BasicDBObject("alphabet", Character.toString(a)));
    }
    long totalCount = collection.count();
    Assertions.assertThat(alphabetCnt).isEqualTo(totalCount);

    DBCursor cursorWithLimit = collection.find().limit(5);
    Assertions.assertThat(5).isEqualTo(cursorWithLimit.size());

    DBCursor cursorWithSkipAndLimit = collection.find().skip(23).limit(5);
    Assertions.assertThat(3).isEqualTo(cursorWithSkipAndLimit.size());
  }

  @Test
  public void queryMapInsteadOfDBObject() {
    collection.findOne(new BasicDBObject("sub", new HashMap<String, String>()));
  }

  @Test
  public void insertMapInsteadOfDBObject() {
    HashMap<String, String> value = new HashMap<String, String>();
    value.put("a", "a");
    collection.insert(new BasicDBObject("sub", value));
    assertEquals(value, collection.findOne().get("sub"));
  }
}
