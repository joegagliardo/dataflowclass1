"""A template to import the default package and parse the arguments"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io import ReadFromAvro, WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class RegionSplit(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        #return [(regionid, regionname)] # ParDo's need to return a list
        yield (regionid, regionname) # Can also use yield instead of returning a list

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflowclass1-bucket/',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default = 'gs://dataflowclass1-bucket/territories_output',      
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will branch and create two output files from one input
  with beam.Pipeline(options=pipeline_options) as p:
    territories =  p | 'Read Territories' >> ReadFromAvro(known_args.input + 'territories.avro')
    print('Keep the data in its current shape and use GroupBy')
    territoriesgroup = (
        territories
          | 'Group Territories' >> beam.GroupBy(lambda x : x['regionid'])
          | 'Print GroupBy' >> beam.Map(print)
    )          

    print('Reshape to a KV tuple')
    territoriestuple = (
       territories
          | 'Reshape Territories to KV' >> beam.Map(lambda x : (x['regionid'], (x['territoryid'], x['territorydescription'])))
    )         
    territoriestuple | 'Print tuple' >> beam.Map(print) 

    print('Use GroupByKey')
    territoriesgroup = (
       territoriestuple
          | 'Group Territories by Key' >> beam.GroupByKey()
    )          
    territoriesgroup | 'Print GroupByKey' >> beam.Map(print)

    regions = (
        p | 'Read Regions' >> ReadFromText(known_args.input + 'regions.csv')
          | 'Split Regions' >> beam.ParDo(RegionSplit())
          | 'Title Case Regions' >> beam.Map(lambda x : (int(x[0]), x[1].title()))
    )

    print('CoGroupByKey')
    join = ({'regions':regions, 'territories':territoriestuple} | beam.CoGroupByKey())
    join | 'Print CoGroupByKey' >> beam.Map(print)



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()


# # using a ParDo and DoFn instead of a Map
# filename = 'regions.csv'
# with beam.Pipeline() as p:
#     lines = p | 'Read' >> ReadFromText(filename)
#     records = lines | 'Split' >> beam.ParDo(RegionSplit())
#     records | 'Write' >> WriteToText('regions.out')

