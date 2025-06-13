"""A template to import the default package and parse the arguments"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re, os, sys

# Get the directory of the current script (your_project/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Construct the path to the 'classes' directory
classes_dir = os.path.join(current_dir, 'models')
# Add the 'classes' directory to sys.path
sys.path.append(classes_dir)
classes_dir = os.path.join(current_dir, 'parsers')
# Add the 'classes' directory to sys.path
sys.path.append(classes_dir)


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from Category import Category
from CategoryCSVParseClass import CategoryCSVParseClass

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  projectid = os.environ.get('PROJECT')
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default=f'gs://{projectid}/regions.csv',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default = f'gs://{projectid}/regions_output',      
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    lines = p | 'Read' >> ReadFromText(known_args.input)
    records = lines | 'Split' >> beam.ParDo(CategoryCSVParseClass())
    records = records | 'output' >> beam.Map(lambda x: x._as_json())
    records | 'Write' >> WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


