package ba.com;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

public class GsonSerde implements Serde<JsonObject> {
    @Override
    public Serializer<JsonObject> serializer() {
        return new Serializer<>() {
            @Override
            public byte[] serialize(String topic, JsonObject data) {
                if (data == null) {
                    return Bytes.EMPTY;
                }
                return data.toString().getBytes();
            }
        };
    }

    @Override
    public Deserializer<JsonObject> deserializer() {

        return new Deserializer<JsonObject>() {
            @Override
            public JsonObject deserialize(String topic, byte[] data) {
                Gson gson = new Gson();
                return gson.fromJson(new String(data), JsonObject.class);
            }
        };
    }
}

