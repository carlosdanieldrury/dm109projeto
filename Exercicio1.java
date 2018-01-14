package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;
import java.util.Scanner;

public class GearChangingCount {

  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );

    System.out.println("Enter your car number to check how many times a car changes gears");
    Scanner sc = new Scanner(System.in);
    int car = sc.nextInt();
 

    stream  .flatMap(new TelemetryJsonParser(car))
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .reduce(new GearChangeReducer())
            .flatMap(new GearMapper())
            .map(new GearPrinter())
            .print();

    env.execute();
    }

    // FlatMap Function - Json Parser
    // Receive JSON data from Kafka broker and parse car number, speed and counter
    
    // {"Car": 9, "time": "52.196000", "telemetry": {"Vaz": "1.270000", "Distance": "4.605865", "LapTime": "0.128001", 
    // "RPM": "591.266113", "Ay": "24.344515", "Gear": "3.000000", "Throttle": "0.000000", 
    // "Steer": "0.207988", "Ax": "-17.551264", "Brake": "0.282736", "Fuel": "1.898847", "Speed": "34.137680"}}

    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Integer, Integer>> {

        private int car;

        public TelemetryJsonParser(int car) {
            this.car = car;
        }

      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
          if (jsonTelemetry.get("Car").asInt() == car) {

            int currentGear = jsonTelemetry.get("telemetry").get("Gear").asInt();

            out.collect(new Tuple3<>(String.valueOf(car),  currentGear, 1));

          }
        
      }
    }

    // Reduce Function - Sum samples and count
    // This funciton return, for each car, the sum of two speed measurements and increment a conter.
    // The counter is used for the average calculation.
    static class GearChangeReducer implements ReduceFunction<Tuple3<String, Integer, Integer>> {
      @Override
      public Tuple3<String,Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) {

            int currentGearCount = value1.f2;
            if (value1.f1 != value2.f1) {
                currentGearCount += 1;
            }

            return new Tuple3<>(value1.f0, value2.f1, currentGearCount);
      }
    }

    // FlatMap Function - Average
    // Calculates the average
    static class GearMapper implements FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, Integer, Integer> carInfo, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(  new Tuple2<>( carInfo.f0, carInfo.f2) );
      }
    }

    // Map Function - Print average    
    static class GearPrinter implements MapFunction<Tuple2<String, Integer>, String> {
      @Override
      public String map(Tuple2<String, Integer> gcEntry) throws Exception {
        return  String.format("Gear changed for car %s : %d times ", gcEntry.f0 , gcEntry.f1 ) + "\n...\n\n" ;
      }
    }

  }
