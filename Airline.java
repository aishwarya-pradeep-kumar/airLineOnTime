package db.test;

import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Airline {

    public enum DataFields {
        YEAR, QUARTER, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, FLIGHT_DATE,
        UNIQUE_CARIER, AIRLINE_ID, CARRIER, TAIL_NUM, FLIGHT_NUM,
        ORIGIN, ORIGIN_CITY_NAME, ORIGIN_STATE, ORIGIN_STATE_FIPS, ORIGIN_STATE_NAME, ORIGIN_WAC,
        DEST, DEST_CITY_NAME, DEST_STATE, DEST_STATE_FIPS, DEST_STATE_NAME, DEST_WAC,
        CRS_DEP_TIME, DEP_TIME, DEP_DELAY, DEP_DELAY_MINUTES, DEP_DEL_15, DEPARTURE_DELAY_GROUPS,
        DEP_TIME_BLK, TAXI_OUT, WHEELS_OFF, WHEELS_ON, TAXI_IN,
        CRS_ARR_TIME, ARR_TIME, ARR_DELAY, ARR_DELAY_MINUTES, ARR_DEL_15, ARRIVAL_DELAY_GROUPS,
        ARR_TIME_BLK, CANCELLED, CANCELLATION_CODE, DIVERTED,
        CRS_ELAPSED_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME, FLIGHTS, DISTANCE, DISTANCE_GROUP,
        CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final static Text one = new Text("1");
        private Text word = new Text();
        private Text out_value = new Text();
        private int line = 0;

        private String[] parse(String str) {
            ArrayList<String> list = new ArrayList<String>();
            int start=0;
            int end=0;
            do {
                String next;
                if (str.charAt(start) == ',') {
                    next = "";
                    start++;
                } else if (str.charAt(start) == '"') {
                    end = str.indexOf('"',start+1);
                    if (end==-1) {
                        next = str.substring(start+1);
                    } else {
                        next = str.substring(start+1,end);
                    }
                    start = end + 2;
                } else {
                    end = str.indexOf(',',start+1);
                    if (end==-1) {
                        next = str.substring(start);
                    } else {
                        next = str.substring(start,end);
                    }
                    start = end + 1;
                }
                list.add(next);
            } while (start < str.length() && end != -1);
            return list.toArray(new String[0]);
        }
        

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            line++;
            String[] values = parse(value.toString());

            if (values[DataFields.YEAR.ordinal()].equalsIgnoreCase("YEAR")) {
                // header, ignore
                return;
            }

            try {
            int cancelled = Double.valueOf(values[DataFields.CANCELLED.ordinal()]).intValue();
            if (cancelled > 0) {
                word.set("cancelled");
                output.collect(word, one);

                word.set("cancelled_year " + values[DataFields.YEAR.ordinal()]);
                output.collect(word, one);

                word.set("cancelled_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                output.collect(word, one);
            } else {
                int depDelMinutes = Double.valueOf(values[DataFields.DEP_DELAY_MINUTES.ordinal()]).intValue();
                word.set("average_dep_delay_overall");
                out_value.set(Integer.toString(depDelMinutes) + " 1");
                output.collect(word, out_value);

                int depDel15 = Double.valueOf(values[DataFields.DEP_DEL_15.ordinal()]).intValue();
                if (depDel15 > 0) {
                    word.set("delayed_departure");
                    output.collect(word, one);

                    word.set("delayed_departure_from " + values[DataFields.ORIGIN.ordinal()]);
                    output.collect(word, one);

                    word.set("average_dep_delay_late");
                    out_value.set(Integer.toString(depDelMinutes) + " 1");
                    output.collect(word, out_value);

                    word.set("delayed_departure_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                    output.collect(word, one);

                    word.set("delayed_departure_from_year_airline " + values[DataFields.ORIGIN.ordinal()] + " " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                    output.collect(word, one);

                    word.set("average_dep_delay_late_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                    out_value.set(Integer.toString(depDelMinutes) + " 1");
                    output.collect(word, out_value);

                    out_value.set(values[DataFields.ORIGIN.ordinal()] + " 1");

                    word.set("top_late_origin");
                    output.collect(word, out_value);

                    word.set("top_late_origin_year " + values[DataFields.YEAR.ordinal()]);
                    output.collect(word, out_value);

                    word.set("top_late_origin_weekday " + values[DataFields.DAY_OF_WEEK.ordinal()]);
                    output.collect(word, out_value);

                    word.set("top_late_origin_weekday_year " + values[DataFields.DAY_OF_WEEK.ordinal()] + " " + values[DataFields.YEAR.ordinal()]);
                    output.collect(word, out_value);
                }
                int diverted = Double.valueOf(values[DataFields.DIVERTED.ordinal()]).intValue();
                if (diverted > 0) {
                    word.set("diverted");
                    output.collect(word, one);

                    word.set("diverted_year " + values[DataFields.YEAR.ordinal()]);
                    output.collect(word, one);

                    word.set("diverted_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                    output.collect(word, one);
                } else {
                    int arrDelMinutes = 0;
                    try {
                        arrDelMinutes = Double.valueOf(values[DataFields.ARR_DELAY_MINUTES.ordinal()]).intValue();
                        word.set("average_arr_delay_overall");
                        out_value.set(Integer.toString(arrDelMinutes) + " 1");
                        output.collect(word, out_value);
                    } catch (NumberFormatException e) {
                        // One record in the dataset has invalid values for this and other fields, so we'l drop this one record
                        System.out.println("caught NumberFormatException when parsing ARR_DELAY_MINUTES");
                        System.out.println("line=" + line);
                        for (int i=0; i<values.length; i++) {
                            System.out.println("vals["+i+"]="+values[i]);
                        }
                    }

                    int arrDel15 = Double.valueOf(values[DataFields.ARR_DEL_15.ordinal()]).intValue();
                    if (arrDel15 > 0 && arrDelMinutes != 0) {
                        word.set("delayed_arrival");
                        output.collect(word, one);

                        word.set("delayed_arrival_to " + values[DataFields.DEST.ordinal()]);
                        output.collect(word, one);

                        word.set("average_arr_delay_late");
                        out_value.set(Integer.toString(arrDelMinutes) + " 1");
                        output.collect(word, out_value);

                        word.set("delayed_arrival_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                        output.collect(word, one);

                        word.set("delayed_arrival_to_year_airline " + values[DataFields.DEST.ordinal()] + " " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                        output.collect(word, one);

                        word.set("average_arr_delay_late_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                        out_value.set(Integer.toString(arrDelMinutes) + " 1");
                        output.collect(word, out_value);

                        out_value.set(values[DataFields.DEST.ordinal()] + " 1");

                        word.set("top_late_destination");
                        output.collect(word, out_value);

                        word.set("top_late_destination_year " + values[DataFields.YEAR.ordinal()]);
                        output.collect(word, out_value);

                        word.set("top_late_destination_weekday " + values[DataFields.DAY_OF_WEEK.ordinal()]);
                        output.collect(word, out_value);

                        word.set("top_late_destination_weekday_year " + values[DataFields.DAY_OF_WEEK.ordinal()] + " " + values[DataFields.YEAR.ordinal()]);
                        output.collect(word, out_value);

                        int carrierDelay = 0;
                        int weatherDelay = 0;
                        int nasDelay = 0;
                        int securityDelay = 0;
                        int lateAircraftDelay = 0;
                        try {
                            carrierDelay = Double.valueOf(values[DataFields.CARRIER_DELAY.ordinal()]).intValue();
                        } catch (NumberFormatException e) {}
                        try {
                            weatherDelay = Double.valueOf(values[DataFields.WEATHER_DELAY.ordinal()]).intValue();
                        } catch (NumberFormatException e) {}
                        try {
                            nasDelay = Double.valueOf(values[DataFields.NAS_DELAY.ordinal()]).intValue();
                        } catch (NumberFormatException e) {}
                        try {
                            securityDelay = Double.valueOf(values[DataFields.SECURITY_DELAY.ordinal()]).intValue();
                        } catch (NumberFormatException e) {}
                        try {
                            lateAircraftDelay = Double.valueOf(values[DataFields.LATE_AIRCRAFT_DELAY.ordinal()]).intValue();
                        } catch (NumberFormatException e) {}

                        if (carrierDelay > 0) {
                            word.set("carrier_delay_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, one);

                            word.set("carrier_delay_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, one);

                            word.set("carrier_delay_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, one);

                            word.set("carrier_delay_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, one);

                            out_value.set(Integer.toString(carrierDelay) + " 1");

                            word.set("carrier_delay_average_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, out_value);

                            word.set("carrier_delay_average_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, out_value);

                            word.set("carrier_delay_average_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, out_value);

                            word.set("carrier_delay_average_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, out_value);
                        }
                        if (weatherDelay > 0) {
                            word.set("weather_delay_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, one);

                            word.set("weather_delay_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, one);

                            word.set("weather_delay_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, one);

                            word.set("weather_delay_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, one);

                            out_value.set(Integer.toString(weatherDelay) + " 1");

                            word.set("weather_delay_average_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, out_value);

                            word.set("weather_delay_average_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, out_value);

                            word.set("weather_delay_average_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, out_value);

                            word.set("weather_delay_average_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, out_value);
                        }
                        if (nasDelay > 0) {
                            word.set("nas_delay_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, one);

                            word.set("nas_delay_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, one);

                            word.set("nas_delay_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, one);

                            word.set("nas_delay_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, one);

                            out_value.set(Integer.toString(nasDelay) + " 1");

                            word.set("nas_delay_average_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, out_value);

                            word.set("nas_delay_average_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, out_value);

                            word.set("nas_delay_average_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, out_value);

                            word.set("nas_delay_average_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, out_value);
                        }
                        if (securityDelay > 0) {
                            word.set("security_delay_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, one);

                            word.set("security_delay_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, one);

                            word.set("security_delay_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, one);

                            word.set("security_delay_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, one);

                            out_value.set(Integer.toString(securityDelay) + " 1");

                            word.set("security_delay_average_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, out_value);

                            word.set("security_delay_average_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, out_value);

                            word.set("security_delay_average_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, out_value);

                            word.set("security_delay_average_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, out_value);
                        }
                        if (lateAircraftDelay > 0) {
                            word.set("lateAircraft_delay_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, one);

                            word.set("lateAircraft_delay_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, one);

                            word.set("lateAircraft_delay_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, one);

                            word.set("lateAircraft_delay_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, one);

                            out_value.set(Integer.toString(lateAircraftDelay) + " 1");

                            word.set("lateAircraft_delay_average_year " + values[DataFields.YEAR.ordinal()]);
                            output.collect(word, out_value);

                            word.set("lateAircraft_delay_average_year_airline " + values[DataFields.YEAR.ordinal()] + " " + "\"" + values[DataFields.UNIQUE_CARIER.ordinal()] + "\"");
                            output.collect(word, out_value);

                            word.set("lateAircraft_delay_average_year_origin " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.ORIGIN.ordinal()]);
                            output.collect(word, out_value);

                            word.set("lateAircraft_delay_average_year_dest " + values[DataFields.YEAR.ordinal()] + " " + values[DataFields.DEST.ordinal()]);
                            output.collect(word, out_value);
                        }
                    }
                }
            }
            } catch (RuntimeException e) {
                System.out.println("caught exception: " + e.getMessage());
                System.out.println("line=" + line);
                for (int i=0; i<values.length; i++) {
                    System.out.println("vals["+i+"]="+values[i]);
                }
                throw e;
            }

            word.set("total");
            output.collect(word, one);
        }
    }

    public static class Pairs implements Comparable<Pairs> {
        public String key;
        public int value;

        public Pairs(String key, int value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Pairs other) {
            if (value < other.value) {
                return 1;
            } else if (value > other.value) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text value = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            if (key.toString().contains("average")) {
                long sum_num = 0, sum_count = 0;
                while (values.hasNext()) {
                    String[] vals = values.next().toString().split(" ");
                    long num, count;
                    try {
                        num = Long.parseLong(vals[0]);
                        count = Long.parseLong(vals[1]);
                    } catch (NumberFormatException e) {
                        System.out.println("combiner: couldn't parse " + vals[0] + " or " + vals[1] + " as long");
                        throw e;
                    }
                    sum_num += num;
                    sum_count += count;
                }
                value.set(Long.toString(sum_num) + " " + Long.toString(sum_count));
                output.collect(key, value);
            } else if (key.toString().startsWith("top")) {
                HashMap<String,Integer> top10map = new HashMap<String,Integer>();
                int sum = 0;
                while (values.hasNext()) {
                    String[] vals = values.next().toString().split(" ");
                    String topkey = vals[0];
                    int count;
                    try {
                        count = Integer.parseInt(vals[1]);
                    } catch (NumberFormatException e) {
                        System.err.println("combiner: couldn't parse " + vals[1] + " as int");
                        continue;
                    }
                    Integer curval = top10map.get(topkey);
                    if (curval == null) curval = 0;
                    curval += count;
                    top10map.put(topkey, curval);
                }
                for (java.util.Map.Entry<String,Integer> entry: top10map.entrySet()) {
                    value.set(entry.getKey() + " " + entry.getValue());
                    output.collect(key, value);
                }
            } else {
                int sum = 0;
                while (values.hasNext()) {
                    String strval = values.next().toString();
                    int num;
                    try {
                        num = Integer.parseInt(strval);
                    } catch (NumberFormatException e) {
                        System.out.println("combiner: couldn't parse " + strval + " as int");
                        throw e;
                    }
                    sum += num;
                }
                value.set(Integer.toString(sum));
                output.collect(key, value);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text value = new Text();

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            if (key.toString().contains("average")) {
                long sum_num = 0, sum_count = 0;
                while (values.hasNext()) {
                    String[] vals = values.next().toString().split(" ");
                    long num, count;
                    try {
                        num = Long.parseLong(vals[0]);
                        count = Long.parseLong(vals[1]);
                    } catch (NumberFormatException e) {
                        System.out.println("combiner: couldn't parse " + vals[0] + " or " + vals[1] + " as long");
                        throw e;
                    }
                    sum_num += num;
                    sum_count += count;
                }
                double average = (double)sum_num / sum_count;
                value.set(Long.toString(sum_num) + " " + Long.toString(sum_count) + " " + Double.toString(average));
                output.collect(key, value);
            } else if (key.toString().startsWith("top")) {
                HashMap<String,Integer> top10map = new HashMap<String,Integer>();
                int sum = 0;
                while (values.hasNext()) {
                    String[] vals = values.next().toString().split(" ");
                    String topkey = vals[0];
                    int count;
                    try {
                        count = Integer.parseInt(vals[1]);
                    } catch (NumberFormatException e) {
                        System.err.println("combiner: couldn't parse " + vals[1] + " as int");
                        continue;
                    }
                    Integer curval = top10map.get(topkey);
                    if (curval == null) curval = 0;
                    curval += count;
                    top10map.put(topkey, curval);
                }
                ArrayList<Pairs> top10list = new ArrayList<Pairs>();
                for (java.util.Map.Entry<String,Integer> entry: top10map.entrySet()) {
                    top10list.add(new Pairs(entry.getKey(), entry.getValue()));
                }
                Collections.sort(top10list);
                StringBuilder outstr = new StringBuilder();
                int top_cnt = 0;
                for (Pairs p: top10list) {
                    outstr.append(p.key).append(" ").append(p.value).append(" ");
                    top_cnt++;
                    if (top_cnt >= 10) break;
                }
                value.set(outstr.toString());
                output.collect(key, value);
            } else {
                int sum = 0;
                while (values.hasNext()) {
                    String strval = values.next().toString();
                    int num;
                    try {
                        num = Integer.parseInt(strval);
                    } catch (NumberFormatException e) {
                        System.out.println("combiner: couldn't parse " + strval + " as int");
                        throw e;
                    }
                    sum += num;
                }
                value.set(Integer.toString(sum));
                output.collect(key, value);
            }
        }
    }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Airline.class);
    conf.setJobName("airline");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Combine.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
