package com.github.fakemongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class FongoCodecRegistryTest {

    public class Pojo {
        public String _id;
        public String value;

        public Pojo() {
        }

        public Pojo(String _id, String value) {
            this._id = _id;
            this.value = value;
        }
    }

    public class PojoCodec implements Codec<Pojo> {
        @Override
        public Pojo decode(BsonReader reader, DecoderContext decoderContext) {
            Pojo pojo = new Pojo();
            reader.readStartDocument();
            pojo._id = reader.readString("_id");
            pojo.value = reader.readString("value");
            reader.readEndDocument();
            return pojo;
        }

        @Override
        public void encode(BsonWriter writer, Pojo value, EncoderContext encoderContext) {
            writer.writeStartDocument();
            writer.writeString("_id", value._id);
            writer.writeString("value", value.value);
            writer.writeEndDocument();
        }

        @Override
        public Class<Pojo> getEncoderClass() {
            return Pojo.class;
        }
    }

    @Test
    public void testSerializeAndDeserializeWithDedicatedCodecShouldWork() {
        CodecRegistry registry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(), CodecRegistries.fromCodecs(new PojoCodec()));
        Fongo fongo = new Fongo("test", Fongo.DEFAULT_SERVER_VERSION, registry);
        Pojo pojo = new Pojo("1", "ok codec");
        MongoCollection<Pojo> col = fongo.getDatabase("test").getCollection("test").withDocumentClass(Pojo.class);
        col.insertOne(pojo);
        Pojo loadedPojo = col.find().first();
        assertEquals(pojo._id, loadedPojo._id);
        assertEquals(pojo.value, loadedPojo.value);
    }

    @Test(expected= CodecConfigurationException.class)
    public void testSerializeAndDeserializeWithoutDedicatedCodecShouldFail() {
        Fongo fongo = new Fongo("test", Fongo.DEFAULT_SERVER_VERSION);
        Pojo pojo = new Pojo("1", "ok codec");
        MongoCollection<Pojo> col = fongo.getDatabase("test").getCollection("test").withDocumentClass(Pojo.class);
        col.insertOne(pojo);
    }


}
