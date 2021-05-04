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
        String regionsOutputFileName = "output/regions";

        PCollection<String> regions = p
            .apply("Read", TextIO.read().from(regionsInputFileName))
            .apply("Parse", MapElements.into(TypeDescriptors.strings()).via((String element) -> element + "*"));
        regions.apply("Write", TextIO.write().to(regionsOutputFileName));
        p.run().waitUntilFinish();
    }
}

