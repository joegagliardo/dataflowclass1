from __future__ import absolute_import

import argparse
import logging
import re

#from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class RegionParseTuple(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        #return [(int(regionid), regionname)] # ParDo's need to return a list
        yield (int(regionid), regionname) # Can also use yield instead of returning a list

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflowclass1-bucket/regions.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default = 'gs://dataflowclass1-bucket/regions_output',      
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
        regionsfilename = 'regions.csv'
        regions = (
            p | 'Read' >> ReadFromText(regionsfilename)
#              | 'Parse' >> beam.ParDo(RegionParseTuple())
        )
        # Branch 1
        (regions 
             | 'Parse 1' >> beam.ParDo(RegionParseTuple())
             | 'Lowercase regions' >> beam.Map(lambda x : (x[0] * 100, x[1].lower()))
             | 'Write' >> WriteToText('regions2.out')
        )
        # Branch 2
        (regions 
             | 'Parse 2' >> beam.ParDo(RegionParseTuple())
             | 'Uppercase regions' >> beam.Map(lambda x : (x[0] * 10, x[1].upper()))
             | 'Print' >> beam.Map(print)
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
         

