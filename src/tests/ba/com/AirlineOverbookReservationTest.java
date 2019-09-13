package ba.com;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class AirlineOverbookReservationTest {

    @org.junit.jupiter.api.Test
    void addTopologyForOverbooking() {

        StreamsBuilder builder = new StreamsBuilder();
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "BA_ON_STRIKE");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        TopologyTestDriver testDriver = new TopologyTestDriver(
                AirlineOverbookReservation.addTopologyForOverbooking(builder).build(), props);

        ConsumerRecordFactory<String, JsonObject> scheduleFactory = new ConsumerRecordFactory<>("airline-schedule",
                new StringSerializer(), new GsonSerde().serializer());

        ConsumerRecordFactory<String, JsonObject> bookingFactory = new ConsumerRecordFactory<>("airline-booking",
                new StringSerializer(), new GsonSerde().serializer());

        JsonObject airlineSchedule = new JsonObject();
        airlineSchedule.addProperty("AircraftCapacity", 4);
        testDriver.pipeInput(scheduleFactory.create("airline-schedule", "BA001", airlineSchedule));

        for (int i = 0; i < 6; i++) {
            JsonObject booking = new JsonObject();
            booking.addProperty("FlightNumber", "BA001");
            testDriver.pipeInput(bookingFactory.create("airline-booking", "BA001", booking));
        }

        ProducerRecord<String, JsonObject> overbooked = testDriver.readOutput("flight-overbooked", new StringDeserializer(), new GsonSerde().deserializer());
        Assert.assertNotNull(overbooked);

    }
}