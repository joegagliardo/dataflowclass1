import apache_beam as beam
from apache_beam.io import ReadFromText

class NestJoin(beam.PTransform):
    '''
    This PTransform will take a dictionary to the left of the | which will be the collection of the two
    PCollections you want to join together. Both must be a dictionary. You will then pass in the name of each
    PCollection and the key to join them on.
    It will automatically reshape the two dicts into tuples of (key, dict) where it removes the key from each dict
    It then CoGroups them and reshapes the tuple into a dict ready for insertion to a BQ table
    '''
    def __init__(self, parent_name, parent_key, child_name, child_key):
        self.parent_name = parent_name
        self.parent_key = parent_key
        self.child_name = child_name
        self.child_key = child_key

    @staticmethod
    def excludeKeysFromDict(d, keyset):
        return {k:v for k,v in d.items() if k in set(d.keys()).difference(keyset)}
        
    @staticmethod
    def reshapeToKV(item, key):
        # pipeline object should be a dictionary
        return (item[key], NestJoin.excludeKeysFromDict(item, {key}))

    def reshapeCoGroupToDict(self, item):
        ret = {self.parent_key: item[0]
              , **item[1][self.parent_name][0]
              , self.child_name: item[1][self.child_name]}
        return ret

    def expand(self, pcols):
        return (
                {
                self.parent_name : pcols[self.parent_name] | f'Convert {self.parent_name} to KV' 
                    >> beam.Map(self.reshapeToKV, self.parent_key)
                ,self.child_name : pcols[self.child_name] | f'Convert {self.child_name} to KV'
                    >> beam.Map(self.reshapeToKV, self.child_key)
                } | f'CoGroupByKey {self.child_name} into {self.parent_name}' >> beam.CoGroupByKey()
                  | f'Reshape to dictionary' >> beam.Map(self.reshapeCoGroupToDict)
               )

class LeftJoin(NestJoin):
    '''
    Overloads the reshapeCoGroupToDict method to flatten out all the children to produce a traditional JOIN result
    '''
    def reshapeCoGroupToDict(self, item):
        ret = [{self.parent_key: item[0]
              , **item[1][self.parent_name][0]
              , **row}
              for row in item[1][self.child_name]]
        return ret

    
    
class RegionParseDict(beam.DoFn):
    def process(self, element):
        regionid, regionname = element.split(',')
        yield {'regionid':int(regionid), 'regionname':regionname.title()}

class TerritoryParseDict(beam.DoFn):
    def process(self, element):
        territoryid, territoryname, regionid = element.split(',')
        yield {'territoryid':int(territoryid), 'territoryname' : territoryname, 'regionid':int(regionid)}
    
regionsfilename = 'regions.csv'
territoriesfilename = 'territories.csv'
PROJECT_ID = 'qwiklabs-gcp-04-4cf93802c378'

with beam.Pipeline() as p:
    regions = (
              p | 'Read Regions' >> ReadFromText(regionsfilename)
                | 'Parse Regions' >> beam.ParDo(RegionParseDict())
                #| 'Print Regions' >> beam.Map(print)
              )
        
    territories = (
                  p | 'Read Territories' >> ReadFromText('territories.csv')
                    | 'Parse Territories' >> beam.ParDo(TerritoryParseDict())
                    #| 'Print Territories' >> beam.Map(print)
                  )

    nestjoin = {'regions':regions, 'territories':territories} | LeftJoin('regions', 'regionid', 'territories', 'regionid')
    nestjoin | 'Print Nest Join' >> beam.Map(print)
