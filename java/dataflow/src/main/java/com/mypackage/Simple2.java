package com.mypackage;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


public class Simple2 {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        String regionsInputFileName = "regions.csv";
        String regionsOutputFileName = "output/regions";

        PCollection<String> regions = p.apply("Read", TextIO.read().from(regionsInputFileName));
        PCollection<String> length = regions.apply(ParDo.of(new WordLength()));
                                                   
//         PCollection<String> length = regions.apply(ParDo.of(new DoFn<String, String>() {
//             @ProcessElement
//             public void precessElement(@Element String word, OutputReceiver<String> out){
//                 out.output(word.toUpperCase());
//             }
//         }));

//        PCollection<Integer> length = regions.apply(ParDo.of(WordLength()));
        length.apply("Write", TextIO.write().to(regionsOutputFileName));
        p.run().waitUntilFinish();
    }
    
    public static class WordLength extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<String> out) {
            out.output(word.toUpperCase());
        }
    }

}


