# not working
# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re, os
import importlib

#from past.builtins import unicode

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

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  projectid = os.environ.get('PROJECT')
  parser = argparse.ArgumentParser()
#   parser.add_argument(
#       '--module',
#       dest='module',
#       help='Name of python file to run, without the .py extension',
#       default = 'test')
# #      required = True)
  parser.add_argument(
      '--input_folder',
      dest='input',
      default=f'gs://{projectid}/',
      help='Input file to process.')
  parser.add_argument(
      '--output_folder',
      dest='output',
      default = f'gs://{projectid}/',      
      help='Output file to write results to.')
  parser.add_argument(
      # We use the save_main_session option True when one or more DoFn's in this
      # workflow rely on global context (e.g., a module imported at module level).
      '--save_main_session',
      dest='save_main_session',
      default=True,
      help='save_main_session')
    
  known_args, pipeline_args = parser.parse_known_args(argv)
  known_args_dict = vars(known_args) # convert to a dict

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
#   pipeline_options = PipelineOptions(additional_args)
#   pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # Dynamically import the module to run
  run_module = importlib.import_module(known_args.module)
  run_module.run(known_args_dict, pipeline_args)
  #run_module.run(**(vars(known_args))

  
  # The pipeline will be run on exiting the with block.
#   with beam.Pipeline(options=pipeline_options) as p:
#         run_module.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


