# python3 simple4.py --var1 joey --var2 gagliardo --runner DataFlowRunner --project $PROJECT --region $REGION --temp_location gs://$BUCKET/tmp/
# not working yet
import apache_beam as beam
import sys
import df_parser
import logging

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# Put your custom code here
class RegionSplitTuple(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        #return [(regionid, regionname)] # ParDo's need to return a list
        yield (int(regionid), regionname) # Can also use yield instead of returning a list

def run():
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(f'{known_args.input_folder}regions.csv')
        records = lines | 'Split' >> beam.ParDo(RegionSplitTuple())
        uppercase = records | 'Uppercase' >> beam.Map(lambda x : (int(x[0]), x[1].upper()))
        uppercase | 'Write' >> WriteToText(f'{known_args.output_folder}regions_out.csv')
        
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  known_args, pipeline_options = df_parser.parse(sys.argv)
  print('known_args', known_args)
  run()


