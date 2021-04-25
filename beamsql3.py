# This code is not running in the notebook
# This example is like example 2 but for a real file 
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam import coders
from apache_beam.transforms.sql import SqlTransform

import typing
import json

class Territory(typing.NamedTuple):
    territoryid: int
    territoryname: str
    regionid: int

coders.registry.register_coder(Territory, coders.RowCoder)
        
#@with_output_types(Territory)
class TerritoryParseClass(beam.DoFn):
    def process(self, element):
        territoryid, territoryname, regionid = element.split(',')
        yield Territory(int(territoryid), territoryname.title(), int(regionid))

        
territoriesfilename = 'territories.csv'
with beam.Pipeline() as p:
    territories = (
                  p | 'Read Territories' >> ReadFromText('territories.csv')
                    | 'Parse Territories' >> beam.ParDo(TerritoryParseClass()).with_output_types(Territory)
#                    | 'Parse Territories' >> beam.ParDo(TerritoryParseClass())
                    | 'SQL Territories' >> SqlTransform("""SELECT regionid, count(*) as `cnt` FROM PCOLLECTION GROUP BY regionid""")
                    | 'Map Territories for Print' >> beam.Map(lambda x : f'{x.regionid} - {x.cnt}')
                    | 'Print SQL' >> beam.Map(print)
                    )
    
#https://www.youtube.com/watch?v=zx4p-UNSmrA
