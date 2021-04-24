import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam import coders
from apache_beam.transforms.sql import SqlTransform

import typing
import json

print('Start')
with beam.Pipeline() as p:
    parent = (
            p | 'create parent' >> beam.Create([(1, 'One'), (2, 'Two')])
              | 'map parent' >> beam.Map(lambda x : beam.Row(parent_id = int(x[0]), parent_name = x[1]))
#              | 'print parent' >> beam.Map(print)
    )

    child = (
            p | 'create child' >> beam.Create([('Uno', 1), ('Due', 2), ('Eins', 1)])
              | 'map child' >> beam.Map(lambda x : beam.Row(child_name = x[0], parent_id = int(x[1])))
#              | 'print child' >> beam.Map(print)

    )
    
    ( {'parent': parent, 'child' : child} | SqlTransform("""SELECT p.parent_id, p.parent_name, c.child_name 
    FROM parent as p 
    INNER JOIN child as c ON p.parent_id = c.parent_id""")
    | 'map join' >> beam.Map(lambda x : f'{x.parent_id} {x.parent_name} {x.child_name}')
    | 'print join' >> beam.Map(print)
    )
print('End')