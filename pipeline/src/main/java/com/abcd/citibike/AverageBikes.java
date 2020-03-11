package com.abcd.citibike;

import java.util.List;
import java.util.ArrayList;

// pipeline 
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

// runner
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.values.KV;
// processing window
import org.apache.beam.sdk.transforms.windowing.Window;

// schema definition
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

// time
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

public class AverageBikes {

	public static interface Options extends DataflowPipelineOptions {

        @Description("Pub/Sub topic name. Name will be formatted to projects/PROJECT_ID/topics/TOPIC_NAME")
        @Default.String("station_ingestion_v2")
        String getTopic();

        void setTopic(String s);

        @Description("BiqQuery table name. Name will be formatted to PROJECT_ID:TABLE_NAME")
        @Default.String("abcd.average_bikes_v1")
        String getBikeTable();

        void setBikeTable(String s);

        @Description("Pub/Sub topic name. Name will be formatted to projects/PROJECT_ID/topics/TOPIC_NAME")
        @Default.String("gs://test-arsh-dataflow-pipeline-v3-sbx/data/output.txt")
        String getDestination();

        void setDestination(String s);
	}

	public static void main(String[] args){
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		String topic =  options.getProject() + "/topics/" + options.getTopic();
		String bikesTable = options.getProject() + options.getBikeTable();

		String destinationPath = options.getDestination();

		Duration window = Duration.standardSeconds(5);

		List<TableFieldSchema> fields = new ArrayList();
		fields.add(new TableFieldSchema().setName("station_id").setType("INT"));
		fields.add(new TableFieldSchema().setName("available_key_id").setType("BOOL"));
		fields.add(new TableFieldSchema().setName("available_bikes").setType("INT"));
		fields.add(new TableFieldSchema().setName("disabled_bikes").setType("INT"));
		fields.add(new TableFieldSchema().setName("available_docks").setType("INT"));
		fields.add(new TableFieldSchema().setName("disabled_docks").setType("INT"));
		fields.add(new TableFieldSchema().setName("rent_status").setType("BOOLEAN"));
		fields.add(new TableFieldSchema().setName("last_reported").setType("TIMESTAMP"));

		TableSchema schema = new TableSchema().setFields(fields);

		PCollection<BikeInfo> initialRecords = p
                .apply("GetMssages", PubsubIO.readStrings().fromTopic(topic))
                .apply("ExtractData", ParDo.of(new DoFn<String, BikeInfo>(){
                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws Exception {
                        String line = ctx.element();
                        ctx.output(BikeInfo.newBikeInfo(line));
                    }
                }));
		PCollection<KV<String,Double>> avgAvailableBikes = initialRecords
                .apply("SlidingWindow", Window.into(SlidingWindows.of(window)))
                .apply("ByLastUpdated",ParDo.of(new DoFn<BikeInfo, KV<String,Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx, BoundedWindow window1) throws Exception{
                        BikeInfo info = ctx.element();
                        String stationID = info.getStationID();
                        int availableBikes = info.getAvailableBikes();
                        String stationIDWithWindow = stationID + "," + window1.maxTimestamp().toString();
                        ctx.output(KV.of(stationIDWithWindow, availableBikes));
                    }
                }))
                .apply("avgAvailableBikes", Mean.perKey());

//
        PCollection<KV<String,Double>> avgAvailableDocks = initialRecords
                .apply("SlidingWindow", Window.into(SlidingWindows.of(window)))
                .apply("ByLastUpdated",ParDo.of(new DoFn<BikeInfo, KV<String,Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx, BoundedWindow window1) throws Exception{
                        BikeInfo info = ctx.element();
                        String stationID = info.getStationID();
                        int availableDocks = info.getAvailableDocks();
                        String stationIDWithWindow = stationID + "," + window1.maxTimestamp().toString();
                        ctx.output(KV.of(stationIDWithWindow,  availableDocks));
                    }
                }))
                .apply("avgAvailableDocks", Mean.perKey());

        PCollection<KV<String,Double>> avgDisabledBikes = initialRecords
                .apply("SlidingWindow", Window.into(SlidingWindows.of(window)))
                .apply("ByLastUpdated",ParDo.of(new DoFn<BikeInfo, KV<String,Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx, BoundedWindow window1) throws Exception{
                        BikeInfo info = ctx.element();
                        String stationID = info.getStationID();
                        int disabledBikes = info.getDisabledBikes();
                        String stationIDWithWindow = stationID + "," + window1.maxTimestamp().toString();
                        ctx.output(KV.of(stationIDWithWindow,  disabledBikes));
                    }
                }))
                .apply("avgDisabledBikes", Mean.perKey());

        PCollection<KV<String,Double>> avgDisabledDocks = initialRecords
                .apply("SlidingWindow", Window.into(SlidingWindows.of(window)))
                .apply("ByLastUpdated",ParDo.of(new DoFn<BikeInfo, KV<String,Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx, BoundedWindow  window1) throws Exception{
                        BikeInfo info = ctx.element();
                        String stationID = info.getStationID();
                        int disabledDocks = info.getDisabledDocks();
                        String stationIDWithWindow = stationID + "," + window1.maxTimestamp().toString();
                        ctx.output(KV.of(stationIDWithWindow,  disabledDocks));
                    }
                }))
                .apply("avgDisabledDocks", Mean.perKey());


        final TupleTag<Double> avgAvailableBikeTag = new TupleTag<Double>();
        final TupleTag<Double> avgDisabledBikeTag = new TupleTag<Double>();
        final TupleTag<Double> avgAvailableDockTag = new TupleTag<Double>();
        final TupleTag<Double> avgDisabledDockTag = new TupleTag<Double>();

        PCollection<KV<String, CoGbkResult>> joinedAverages = KeyedPCollectionTuple.of(avgAvailableBikeTag, avgAvailableBikes)
                .and(avgAvailableDockTag,avgAvailableDocks).and(avgDisabledBikeTag, avgDisabledBikes)
                .and(avgDisabledDockTag, avgDisabledDocks).apply(CoGroupByKey.<String>create());

        PCollection<String> joinedAveragesBuilt = joinedAverages
                .apply("CombineCoGbResult", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) throws Exception{
                        KV<String, CoGbkResult> e = ctx.element();
                        CoGbkResult result = e.getValue();
                        String name = e.getKey();
                        if (name == null){
                            return;
                        }
                        String stationID = name.split(",")[0];
                        String window = name.split(",")[1];
                        Double avgAvailableBike = result.getOnly(avgAvailableBikeTag);
                        Double avgDisabledBike = result.getOnly(avgDisabledBikeTag);
                        Double avgAvailableDock = result.getOnly(avgAvailableDockTag);
                        Double avgDisabledDock = result.getOnly(avgDisabledDockTag);

                        String line = stationID  + "," + avgAvailableBike +
                                "," + avgDisabledBike + "," + avgAvailableDock +
                                "," + avgDisabledDock + "," + window;
                        ctx.output(line);
                        }
                    }));

        PCollection<AvgBikeInfo> avgBikeInfoPCollection = joinedAveragesBuilt.apply("buildDataModel",ParDo.of(new DoFn<String, AvgBikeInfo>(){
            @ProcessElement
            public void processElement(ProcessContext ctx) throws Exception {
                String line = ctx.element();
                AvgBikeInfo avgBikeInfo = AvgBikeInfo.newAvgBikeInfo(line);
                ctx.output(avgBikeInfo);
            }
        }));
        
        avgBikeInfoPCollection.apply("ToBQRow", ParDo.of(new DoFn<AvgBikeInfo, TableRow>(){
            @ProcessElement
            public void processElement(ProcessContext ctx) throws Exception{
                TableRow row = new TableRow();
                AvgBikeInfo bikeInfo = ctx.element();
                row.set("station_id", bikeInfo.getTimestamp());
                row.set("available_bikes", bikeInfo.getAvgAvailableBikes());
                row.set("disabled_bikes", bikeInfo.getAvgDisabledBikes());
                row.set("available_docks", bikeInfo.getAvgAvailableDocks());
                row.set("disabled_docks", bikeInfo.getAvgDisabledDocks());
                row.set("timestamp", bikeInfo.getTimestamp());
                ctx.output(row);
            }
     })).apply(BigQueryIO.writeTableRows().to(bikesTable).withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        
        joinedAveragesBuilt.apply("WriteMyFile", TextIO.write().to(destinationPath).withWindowedWrites().withoutSharding());
        p.run();
	}
}
