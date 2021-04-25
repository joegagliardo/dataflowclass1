import apache_beam as beam
from apache_beam import coders
from apache_beam.transforms.sql import SqlTransform

import typing

class Child(typing.NamedTuple):
    child_name: str
    parent_id: int
beam.coders.registry.register_coder(Child, beam.coders.RowCoder)
        
print('Before with')
with beam.Pipeline() as p:
    child = (
            p | 'create child' >> beam.Create([('Uno', 1), ('Due', 2), ('Eins', 1), ('Una', 1), ('Dos', 2)])
              | 'map child' >> beam.Map(lambda x : Child(child_name = x[0], parent_id = x[1])).with_output_types(Child)
              | 'sql child' >> SqlTransform("""SELECT 10 * parent_id as parent_id, upper(child_name) as child_name from PCOLLECTION""")
              | 'print map' >> beam.Map(lambda x : f'{x.parent_id} = {x.child_name}')
              | 'print sql' >> beam.Map(print)
    )
print('After with')
    
    