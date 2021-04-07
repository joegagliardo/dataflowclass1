import apache_beam as beam

with beam.Pipeline() as p:
    lines = (
        p | 'Create' >> beam.Create(['one', 'two', 'three', 'four'])
          | 'Uppercase' >> beam.Map(str.upper)
          | 'Print' >> beam.Map(print)
    )

