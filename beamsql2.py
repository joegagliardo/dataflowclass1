# This code is not running in the notebook
# This example uses a simple class to handle the schemas
import apache_beam as beam
from apache_beam import coders
from apache_beam.transforms.sql import SqlTransform

import typing


class Parent(typing.NamedTuple):
    parent_id: int
    parent_name: str
beam.coders.registry.register_coder(Parent, beam.coders.RowCoder)

class Child(typing.NamedTuple):
    child_name: str
    parent_id: int
beam.coders.registry.register_coder(Child, beam.coders.RowCoder)
        

with beam.Pipeline() as p:
    parent = (
            p | 'Create Parent' >> beam.Create([(1, 'One'), (2, 'Two')])
              | 'Map Parent' >> beam.Map(lambda x : Parent(parent_id = x[0], parent_name = x[1])).with_output_types(Parent)
              | 'Map for Print' >> beam.Map(print)
    )

    child = (
            p | 'Create Child' >> beam.Create([('Uno', 1), ('Due', 2), ('Eins', 1), ('Una', 1), ('Dos', 2)])
              | 'Map Child' >> beam.Map(lambda x : Child(child_name = x[0], parent_id = x[1])).with_output_types(Child)
#               | 'SQL Child' >> SqlTransform("""SELECT 10 * parent_id as parent_id, upper(child_name) as child_name from PCOLLECTION""")
#               | 'Print Map' >> beam.Map(lambda x : f'{x.parent_id} = {x.child_name}')
              | 'SQL Child' >> SqlTransform("""SELECT parent_id, count(*) as cnt from PCOLLECTION GROUP BY parent_id""")
              | 'Map for Print' >> beam.Map(lambda x : f'{x.parent_id} = {x.cnt}')
              | 'Print SQL' >> beam.Map(print)
    )
