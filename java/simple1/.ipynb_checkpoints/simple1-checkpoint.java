// This is a prototype and java code will follow for all examples soon
package com.mypackage.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    public static void main(String[] args) {
        // Create the pipeline.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        // Create the PCollection 'lines' by applying a 'Read' transform.
        string regionsInputFileName = "regions.csv";
        string regionsOutputFileName = "regions_out_java.csv";

        PCollection<String> regions = p
            .apply("Read", TextIO.read().from(regionsInputFileName))
            .apply("Parse", MapElements.into(TypeDescriptors.strings()).via(element -> Arrays.asList(element.split("."))))
            .apply("Write", TextIO.write().to(regionsOutputFileName));
        p.run().waitUntilFinish();

    }
}
                   