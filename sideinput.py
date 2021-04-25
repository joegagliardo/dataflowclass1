
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
        yield {'regionid': int(regionid), 'regionname': regionname.title()}

class TerritoryParseTuple(beam.DoFn):
    def process(self, element):
        territoryid, territoryname, regionid = element.split(',')
        yield(int(territoryid), territoryname, int(regionid))
        
                
class LookupRegion(beam.DoFn):
    def process(self, element, lookuptable = [{'regionid':1, 'regionname':'North'}, {'regionid':2, 'regionname':'South'}]):
        territoryid, territoryname, regionid = element
        # Becase the regions PCollection is a different shape, use the following comprehension to make it easier to do a lookup
        lookup = {e['regionid'] : e['regionname'] for e in lookuptable }
        yield(territoryid, territoryname, regionid, lookup.get(regionid, 'No Region'))

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

    territories =  (
        p | 'Read Territories' >> ReadFromText(f'{known_args.input}/territories.csv')
          | 'Parse Territories' >> beam.ParDo(TerritoryParseTuple())
    )

    lookup = (
        territories
        | beam.ParDo(LookupRegion(), lookuptable = beam.pvalue.AsList(regions))
        | 'Write' >> WriteToText(f'{known_args.output}/sideinputs.csv')
    )
    p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()

