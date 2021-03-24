"""A template to import the default package and parse the arguments"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io import ReadFromAvro, WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud import storage

class ParseProductsJson(beam.DoFn):
    def process(self, item):
        import json
        data = json.loads(item)
        productid = data.get('productid')
        productname = data.get('productname')
        categoryid = data.get('categoryid')
        unitsinstock = data.get('unitsinstock')
        unitprice = data.get('unitprice')

        yield (productid, productname, categoryid, unitsinstock, unitprice)

class CombineProductsByKey(beam.CombineFn):
  def create_accumulator(self):
    return (0, 0, 0.0) # cnt, qty, value

  def add_input(self, cnt_qty_value, input):
    cnt, qty, value = cnt_qty_value
    newqty, price = input 
    return (cnt + 1, qty + newqty, value + newqty * price)

  def merge_accumulators(self, accumulators):
    cnt, qty, value = zip(*accumulators)
    return (sum(cnt), sum(qty), sum(value))

  def extract_output(self, cnt_qty_value):
    cnt, qty, value = cnt_qty_value
    return (cnt, qty, value)

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflowclass1-bucket/',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default = 'gs://dataflowclass1-bucket/territories_output',      
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will branch and create two output files from one input

  with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read Products' >> ReadFromText(known_args.input + 'products.json')
         | 'Parse Products JSON' >> beam.ParDo(ParseProductsJson(known_args.input + 'products.json'))
         | 'Reshape Products as KV' >> beam.Map(lambda x : (x[2], (x[3], x[4]))) # 2 is categoryid 3 is unitsinstock 4 is unitprice
         | 'Aggregate Products' >> beam.CombinePerKey(CombineProductsByKey())
         | 'Print Products' >> beam.Map(print)
         )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

