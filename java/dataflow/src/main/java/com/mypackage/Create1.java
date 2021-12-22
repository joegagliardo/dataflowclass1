package com.mypackage;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.io.TextIO;
import java.util.*;
//import org.apache.beam.contrib.io.ConsoleIO;

public class Create1 {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        
        List<String> strings = Arrays.asList("one", "two", "three", "four");

        PCollection<String> lines = p
            .apply("Create", Create.of(strings));
//             .apply("Print", ConsoleIO.Write.create());

//             p.run().waitUntilFinish();
    }
}

