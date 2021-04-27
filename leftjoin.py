# python leftjoin.py --runner Dataflowrunner --project $PROJECT --region us-central1 --temp_location gs://$PROJECT/tmp

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re, os

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io import ReadFromAvro, WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class RegionSplit(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        yield {'regionid':int(regionid), 'regionname':regionname.title()}

class UnnestCoGrouped(beam.DoFn):
    def process(self, item, child_pipeline, parent_pipeline):
        k, v = item
        child_dict = v[child_pipeline]
        parent_dict = v[parent_pipeline]
        for child in child_dict:
            try:
                child.update(parent_dict[0])
                yield child
            except IndexError:
                yield child

class LeftJoin(beam.PTransform):
    def __init__(self, parent_pipeline_name, parent_data, parent_key, child_pipeline_name, child_data,  child_key):
        self.parent_pipeline_name = parent_pipeline_name
        self.parent_data = parent_data
        self.parent_key = parent_key
        self.child_pipeline_name = child_pipeline_name
        self.child_data = child_data
        self.child_key = child_key

    def expand(self, pcols):
        def _format_as_common_key_tuple(child_dict, child_key):
            return (child_dict[child_key], child_dict)

        return ({
                pipeline_name: pcol1 | f'Convert to ({self.parent_key} = {self.child_key}, object) for {pipeline_name}' 
                >> beam.Map(_format_as_common_key_tuple, self.child_key)
                for (pipeline_name, pcol1) in pcols.items()}
                | f'CoGroupByKeey {pcols.keys()}' >> beam.CoGroupByKey()
                | 'Unnest Cogrouped' >> beam.ParDo(UnnestCoGrouped(), self.child_pipeline_name, self.parent_pipeline_name)
        )


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  projectid = os.environ.get('PROJECT')
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default=f'gs://{projectid}/',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default = f'gs://{projectid}/territories_output',      
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will branch and create two output files from one input
  with beam.Pipeline(options=pipeline_options) as p:
    regions = (
        p | 'Read Regions' >> ReadFromText(known_args.input + 'regions.csv')
          | 'Split Regions' >> beam.ParDo(RegionSplit())
    )
    regions | 'print regions' >> beam.Map(print)

    territories =  p | 'Read Territories' >> ReadFromAvro(known_args.input + 'territories.avro')
    territories | 'print territories' >> beam.Map(print)


    leftjoin = {'regions':regions, 'territories':territories} | LeftJoin('regions', regions, 'regionid', 'territories', territories, 'regionid')
    #leftjoin | 'print left join' >> beam.Map(print)

    schema = {
                "namespace": "leftjoin.avro",
                "type": "record",
                "name": "leftjoin",
                "fields": [
                    {"name": "regionid", "type": "int"},
                    {"name": "regionname",  "type": "string"},
                    {"name": "territoryid", "type": ["string", "null"]},
                    {"name": "territorydescription", "type": ["string", "null"]}
                ]
            }

    leftjoin | 'write to leftjoin to file' >> WriteToAvro(known_args.output+'_leftjoin_avro', schema = schema)

#     joined = (
#             p | 'read joined avro file to confirm' >> ReadFromAvro(known_args.output+'_leftjoin_avro*')
#               | 'print joined avro file' >> beam.Map(print)
#             )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

