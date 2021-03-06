"""A template to import the default package and parse the arguments"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class RegionSplit(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        #return [(regionid, regionname)] # ParDo's need to return a list
        yield (regionid, regionname) # Can also use yield instead of returning a list

from apache_beam.options.pipeline_options import PipelineOptions

class MyPipelineOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input',
        dest='input',
        help='Input for the pipeline',
        default='gs://dataflowclass1-bucket/regions.csv')
    parser.add_argument(
        '--output',
        dest='output',
        help='Output for the pipeline',
        default = 'gs://dataflowclass1-bucket/regions_output')
    #known_args, pipeline_args = parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = MyPipelineOptions()
  
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    lines = p | 'Read' >> ReadFromText(known_args.input)
    records = lines | 'Split' >> beam.ParDo(RegionSplit())
    uppercase = records | 'Uppercase' >> beam.Map(lambda x : (int(x[0]), x[1].upper()))
    uppercase | 'Write' >> WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


# using a ParDo and DoFn instead of a Map
filename = 'regions.csv'
with beam.Pipeline() as p:
    lines = p | 'Read' >> ReadFromText(filename)
    records = lines | 'Split' >> beam.ParDo(RegionSplit())
    records | 'Write' >> WriteToText('regions.out')



# options = PipelineOptions()
# google_cloud_options = options.view_as(GoogleCloudOptions)
# google_cloud_options.project = 'my-project-id'
# google_cloud_options.job_name = 'myjob'
# google_cloud_options.staging_location = 'gs://your-bucket-name-here/staging'
# google_cloud_options.temp_location = 'gs://your-bucket-name-here/temp'
# options.view_as(StandardOptions).runner = 'DataflowRunner'

