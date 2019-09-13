package ba.com;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;

public class AirlineSeatReservation {

    public static StreamsBuilder addTopologyForSeatReservation(StreamsBuilder streamsBuilder) {
        var stream = streamsBuilder.stream(
                "seat-events",
                Consumed.with(Serdes.String(), new GsonSerde())
        );

        StoreBuilder countStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("seat-history-state"),
                        Serdes.String(),
                        new GsonSerde()
                );

        stream.process(new ProcessorSupplier<String, JsonElement>() {
            @Override
            public Processor<String, JsonElement> get() {
                return new Processor<String, JsonElement>() {
                    private KeyValueStore store;

                    @Override
                    public void init(ProcessorContext context) {
                        store = (KeyValueStore) context.getStateStore("seat-history-state");
                        context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, (timestamp -> {
                            KeyValueIterator<String, Long> iter = this.store.all();
                            while (iter.hasNext()) {
                                KeyValue<String, Long> entry = iter.next();
                                context.forward(entry.key, entry.value.toString());
                            }

                            iter.close();

                            // commit the current processing progress
                            context.commit();
                        }));
                    }

                    @Override
                    public void process(String key, JsonElement value) {
                        String flightNumber = value.getAsJsonObject().get("FlightNumber").getAsString();
                        String seatNumber = value.getAsJsonObject().get("SeatNumber").getAsString();

                        JsonObject object = (JsonObject) store.get(flightNumber);
                        if (object == null) {
                            object = new JsonObject();
                            object.addProperty("FlightNumber", flightNumber);
                            object.add("seats", new JsonArray());
                        }

                        object.getAsJsonArray("seats").add(value);
                        int seatCount = object.get("SeatCount").getAsInt() + 1;
                        object.addProperty("SeatCount", seatCount);

                        store.put(flightNumber, object);
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        }, new String[]{});

        return streamsBuilder;
    }
}
