package com.mypackage;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.io.TextIO;


public class Simple1 {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        String regionsInputFileName = "regions.csv";
        String regionsOutputFileName = "regions_out_java";

        PCollection<String> regions = p
            .apply("Read", TextIO.read().from(regionsInputFileName))
            .apply("Parse", MapElements.into(TypeDescriptors.strings()).via((String element) -> element + "*"));
        regions.apply("Write", TextIO.write().to(regionsOutputFileName));
        p.run().waitUntilFinish();
    }
}


/*
package com.mypackage;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;


public class Simple1 {
//    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    public static void main(String[] args) {
        // Create the pipeline.
//        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
//        Pipeline p = Pipeline.create(options);
        Pipeline p = Pipeline.create();

        // Create the PCollection 'lines' by applying a 'Read' transform.
        String regionsInputFileName = "regions.csv";
        String regionsOutputFileName = "regions_out_java";

        PCollection<String> regions = p
            .apply("Read", TextIO.read().from(regionsInputFileName))
            .apply("Parse", MapElements.into(TypeDescriptors.strings()).via((String element) -> element + "*"));
        regions.apply("Write", TextIO.write().to(regionsOutputFileName));
        p.run().waitUntilFinish();
    }
}

//ArrayList.asList(element.split("."))))
*/
