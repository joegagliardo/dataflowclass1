
# python sideinput.py --runner Dataflowrunner --project $PROJECT --region us-central1 --temp_location gs://$PROJECT/tmp
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

class RegionParseDict(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        yield {'regionid':int(regionid), 'regionname':regionname.title()}

class TerritoryParseDict(beam.DoFn):
    def process(self, element):
        territoryid, territoryname, regionid = element.split(',')
        yield {'territoryid':int(territoryid), 'territoryname' : territoryname, 'regionid':int(regionid)}

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

def main(argv=None, save_main_session=False):
  projectid = 'qwiklabs-gcp-04-c21b49858f60'
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default=f'gs://{projectid}',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default = f'gs://{projectid}/regions_output',      
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  #  pipeline_options = PipelineOptions(pipeline_args, job_name=f'{projectid}-22')
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    regions = (
              p | 'Read Regions' >> ReadFromText(f'{known_args.input}/regions.csv')
                | 'Parse Regions' >> beam.ParDo(RegionParseDict())
              )
        
    territories = (
                  p | 'Read Territories' >> ReadFromText(f'{known_args.input}/territories.csv')
                    | 'Parse Territories' >> beam.ParDo(TerritoryParseDict())
                  )

    leftjoin = (
               {'regions':regions, 'territories':territories} 
                | LeftJoin('regions', regions, 'regionid', 'territories', territories, 'regionid')
                | 'Write' >> WriteToText(f'{known_args.output}/cogroup.csv')
               )
    p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()


    
import apache_beam as beam
from apache_beam.io import ReadFromText

        

    