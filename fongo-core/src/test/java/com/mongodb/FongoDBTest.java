package com.mongodb;

import com.github.fakemongo.Fongo;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.connection.ServerVersion;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.util.StringUtils;

/**
 * @author Anton Bobukh <abobukh@yandex-team.ru>
 */
public class FongoDBTest {

  private final ReadPreference preference = ReadPreference.nearest();

  @Rule
  public FongoRule fongoRule = new FongoRule(!true, Fongo.V3_SERVER_VERSION);
  private DB db;

  @Before
  public void setUp() {
    db = fongoRule.getDB();
  }

  @Test
  public void commandGetLastErrorAliases() {
    BasicDBObject command;

    command = new BasicDBObject("getlasterror", 1);
    Assert.assertTrue(db.command(command, preference).containsField("ok"));

    command = new BasicDBObject("getLastError", 1);
    Assert.assertTrue(db.command(command, preference).containsField("ok"));
  }

  @Test
  public void commandFindAndModifyAliases() {
    BasicDBObject command = new BasicDBObject("findandmodify", "test").append("remove", true);
    CommandResult commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsEntry("ok", 1.0).containsKey("value");

    command = new BasicDBObject("findAndModify", "test").append("remove", true);
    commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsEntry("ok", 1.0).containsKey("value");
  }

  @Test
  public void commandFindAndModifyNeedRemoveOrUpdate() {
    BasicDBObject command = new BasicDBObject("findandmodify", "test");
    CommandResult commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsEntry("ok", .0).containsEntry("errmsg", "need remove or update");
  }

  @Test
  public void commandBuildInfoAliases() {
    BasicDBObject command;

    command = new BasicDBObject("buildinfo", 1);
    Assert.assertTrue(db.command(command, preference).containsField("version"));

    command = new BasicDBObject("buildInfo", 1);
    Assert.assertTrue(db.command(command, preference).containsField("version"));
  }

  @Test
  public void commandBuildInfoPullsDefaultMongoVersionFromFongo() throws Exception {
    CommandResult commandResult = db.command("buildInfo");
    String expectedVersionAsString = StringUtils.collectionToDelimitedString(fongoRule.getServerVersion().getVersionList(), ".");
    assertThat(commandResult.getString("version")).isEqualTo(expectedVersionAsString);
  }

  @Test
  public void commandBuildInfoPullsChangedMongoVersionFromFongo() throws Exception {
    db = new Fongo("testfongo", new ServerVersion(3, 1)).getDB("testdb");
    CommandResult commandResult = db.command("buildInfo");
    assertThat(commandResult.getString("version")).isEqualTo("3.1.0");
  }

  @Test
  @Ignore("not sure to understant the test")
  public void commandMapReduceAliases() {
    BasicDBObject command;

    command = new BasicDBObject("mapreduce", "test").append("out", new BasicDBObject("inline", 1));
    CommandResult commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsKey("results");
    assertThat(db.command(command, preference).toMap()).containsKey("results");

    command = new BasicDBObject("mapReduce", "test").append("out", new BasicDBObject("inline", 1));
    commandResult = db.command(command, preference);
    assertThat(commandResult.toMap()).containsKey("results");
  }

  @Test
  public void commandStats() throws Exception {
    db = new Fongo("testfongo", new ServerVersion(3, 1)).getDB("testdb");
    CommandResult commandResult = db.getStats();
    assertThat(commandResult.isEmpty()).isTrue();
  }
}
