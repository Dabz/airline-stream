package ba.com;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class AirlineOverbookReservation {

    public static StreamsBuilder addTopologyForOverbooking(StreamsBuilder streamsBuilder) {
        var airlineScheduleTable = streamsBuilder.table(
                "airline-schedule",
                Materialized.as("airline-schedule-table").with(Serdes.String(), new GsonSerde()));

        var airlineBooking = streamsBuilder.stream(
                "airline-booking",
                Consumed.with(Serdes.String(), new GsonSerde()));

        var airlineBookingWithFlithNumberAsKey = airlineBooking.map((k, v) -> {
            return KeyValue.pair(v.get("FlightNumber").getAsString(), v);
        }).through("airline-overbooking-repartition", Produced.with(Serdes.String(), new GsonSerde()));

        var airlineBookingCountPerFlightNumber = airlineBookingWithFlithNumberAsKey.groupByKey().count();

        var flightNumberOverbooked = airlineBookingCountPerFlightNumber.join(airlineScheduleTable, (bookingCount, schedule) -> {
            schedule.add("BookingCount", new Gson().toJsonTree(bookingCount));
            return schedule;
        }).filter((k, v) -> {
            var aircraftCapacity = v.get("AircraftCapacity").getAsInt();
            var bookingCount = v.get("BookingCount").getAsInt();
            return bookingCount > aircraftCapacity;
        });

        flightNumberOverbooked.toStream().to("flight-overbooked",
                Produced.with(Serdes.String(), new GsonSerde()));

        return streamsBuilder;
    }
}
