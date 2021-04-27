# not working
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def parse(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  import argparse, os
    
  projectid = os.environ.get('PROJECT')
  region = os.environ.get('REGION')
  
  temp_location = f'gs://projectid/TMP'
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_folder',
      dest='input_folder',
      default=f'gs://{projectid}/',
      help='Input file to process.')
  parser.add_argument(
      '--output_folder',
      dest='output_folder',
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
# python3 simple4.py --var1 joey --var2 gagliardo --runner DataFlowRunner --project $PROJECT --region $REGION --temp_location gs://$BUCKET/tmp/

  if 'DataFlowRunner' in pipeline_args and 'project' not in pipeline_args:
        # set up standard defaults to simplify calling the job on DataFlowRunner
        pipeline_args.extend(['project', projectid, 'region', region, 'temp_location', f'gs://{projectid}/tmp/'])
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = known_args.save_main_session

  return(known_args, pipeline_options)