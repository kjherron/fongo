package com.mongodb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.bson.BsonDocument;

public class FongoBulkWriteCombiner {

  private final WriteConcern writeConcern;

  private int insertedCount = 0;
  private int matchedCount = 0;
  private int removedCount = 0;
  private int modifiedCount = 0;

  private final Set<com.mongodb.BulkWriteUpsert> upserts = new TreeSet<com.mongodb.BulkWriteUpsert>(new Comparator<com.mongodb.BulkWriteUpsert>() {
    @Override
    public int compare(final com.mongodb.BulkWriteUpsert o1, final com.mongodb.BulkWriteUpsert o2) {
      return (o1.getIndex() < o2.getIndex()) ? -1 : ((o1.getIndex() == o2.getIndex()) ? 0 : 1);
    }
  });
  private final TreeSet<WriteError> errors = new TreeSet<WriteError>(new Comparator<WriteError>() {
    @Override
    public int compare(final WriteError o1, final WriteError o2) {
      return (o1.getIndex() < o2.getIndex()) ? -1 : ((o1.getIndex() == o2.getIndex()) ? 0 : 1);
    }
  });

  public FongoBulkWriteCombiner(WriteConcern writeConcern) {
    this.writeConcern = writeConcern;
  }

  public void addReplaceResult(int idx, WriteResult wr) {
    // Same logic applies
    addUpdateResult(idx, wr);
  }

  public void addUpdateResult(int idx, WriteResult wr) {
    if (wr.isUpdateOfExisting()) {
      matchedCount += wr.getN();
      modifiedCount += wr.getN();
    } else {
      if (wr.getUpsertedId() != null) {
        upserts.add(new BulkWriteUpsert(idx, wr.getUpsertedId()));
//            insertedCount++;
      }
    }
  }

  public void addRemoveResult(WriteResult wr) {
    matchedCount += wr.getN();
    removedCount += wr.getN();
  }

  public void addInsertResult(WriteResult wr) {
    insertedCount += wr.getN();
  }

  public void addInsertError(int idx, WriteConcernException exception) {
    errors.add(new WriteError(idx, exception));
  }

  public BulkWriteResult getBulkWriteResult(WriteConcern writeConcern) {
    if (!writeConcern.isAcknowledged()) {
      return new UnacknowledgedBulkWriteResult();
    }
    return new AcknowledgedBulkWriteResult(insertedCount, matchedCount, removedCount, modifiedCount, new ArrayList<BulkWriteUpsert>(upserts));
  }

  public void throwOnError(ServerAddress address) {
    if (!errors.isEmpty()) {
      List<BulkWriteError> bwErrors = new ArrayList<BulkWriteError>();
      for (WriteError e: errors) {
        bwErrors.add(new BulkWriteError(e.code, e.message, FongoDBCollection.dbObject(e.details), e.index));
      }
      BulkWriteResult bulkWriteResult = getBulkWriteResult(writeConcern);
      throw new BulkWriteException(bulkWriteResult, bwErrors, null, address);
    }
  }

  public static class WriteError {

    private final int index;
    private final int code;
    private final String message;
    private final BsonDocument details;
    private final WriteConcernException exception;

    public WriteError(int index, WriteConcernException exception) {
      this.index = index;
      this.exception = exception;
      this.code = exception.getErrorCode();
      this.message = exception.getErrorMessage();
      this.details = exception.getResponse();
    }

    public int getIndex() {
      return index;
    }

    public int getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }

    public BsonDocument getDetails() {
      return details;
    }

    public WriteConcernException getException() {
      return exception;
    }

  }

}
