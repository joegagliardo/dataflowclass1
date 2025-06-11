from __future__ import absolute_import

import argparse
import logging
import re, os

#from past.builtins import unicode

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam import pvalue
from apache_beam.io import ReadFromText, WriteToText
import typing
from apache_beam.transforms.sql import SqlTransform

class Region(typing.NamedTuple):
    regionid: int
    regionname: str
        
beam.coders.registry.register_coder(Region, beam.coders.RowCoder)
        
class RegionParseClass(beam.DoFn):
    def process(self, element):
        yield Region(int(element['regionid']), element['regiondescription'])
        # yield Region(**element)

class Territory(typing.NamedTuple):
    territoryid: int
    territoryname: str
    regionid: int
beam.coders.registry.register_coder(Territory, beam.coders.RowCoder)
        
class TerritoryParseClass(beam.DoFn):
    def process(self, element):
        yield Territory(int(element['territoryid']), element['territorydescription'], int(element['regionid']))

class Result(typing.NamedTuple):
    regionid: int
    regionname: str
    cnt: int
beam.coders.registry.register_coder(Result, beam.coders.RowCoder)
               

    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  projectid = os.environ.get('PROJECT')
  parser = argparse.ArgumentParser()
  parser.add_argument('--rfile', required=True, help='Path to regions.avro file.')
  parser.add_argument('--territoryfile', required=True, help='Path to territories.avro file.')
  parser.add_argument('--output', required=True, help='GCS path for output.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  print('Known', known_args, 'Pipeline', pipeline_args)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
        regions = (p | 'Read Regions' >> beam.io.ReadFromAvro(known_args.rfile)
                         | 'Parse Regions' >> beam.ParDo(RegionParseClass())
                      )
        territories = (p | 'Read Territories' >> beam.io.ReadFromAvro(known_args.territoryfile)
                         | 'Parse Territories' >> beam.ParDo(TerritoryParseClass())
                      )

        result = ( {'r': regions, 't' : territories} 
             | SqlTransform(
     """
    SELECT r.regionid AS regionid, r.regionname AS regionname, SUM(1) AS cnt 
    FROM r 
    JOIN t on t.regionid = r.regionid 
    GROUP BY r.regionid, r.regionname
    """)
             # | 'Convert to Result Class' >> beam.Map(lambda x : Result(x.regionid, x.regionname, x.cnt))
             # | 'Format Output' >> beam.Map(lambda x : f'{x.regionid}, {x.regionname}, {x.cnt}')
                 )
        result | 'Write' >> WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
    