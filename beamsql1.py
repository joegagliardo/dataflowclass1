# This code is not running in the notebook
# This example just uses a basic Row object
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam import coders
from apache_beam.transforms.sql import SqlTransform
from apache_beam.io import ReadFromText, WriteToText

import typing
import json

with beam.Pipeline() as p:
    parent = (
            p | 'Create Parent' >> beam.Create([(1, 'One'), (2, 'Two')])
              | 'Map Parent' >> beam.Map(lambda x : beam.Row(parent_id = x[0], parent_name = x[1]))
    )

    child = (
            p | 'Create Child' >> beam.Create([('Uno', 1), ('Due', 2), ('Eins', 1), ('Una', 1), ('Dos', 2)])
              | 'Map Child' >> beam.Map(lambda x : beam.Row(child_name = x[0], parent_id = x[1]))
    )
    
    ( {'parent': parent, 'child' : child} 
         | SqlTransform("""
             SELECT p.parent_id, p.parent_name, c.child_name 
             FROM parent as p 
             INNER JOIN child as c ON p.parent_id = c.parent_id
             """)
        | 'Map Join' >> beam.Map(lambda x : f'{x.parent_id} {x.parent_name} {x.child_name}')
#        | 'Print Join' >> beam.Map(print)
        | 'Write' >> WriteToText('parentchild.out')

        )

#     parent | 'print parent' >> beam.Map(print)
#     child  | 'print child' >> beam.Map(print)
