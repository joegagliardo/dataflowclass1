import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

filename = 'regions.csv'
with beam.Pipeline() as p:
    regions = (
        p | 'Read' >> ReadFromText(filename)
          | 'Split' >> beam.Map(lambda x : tuple(x.split(',')))
          | 'Transform' >> beam.Map(lambda x : (int(x[0]) * 10, x[1].upper()))
          | 'Write' >> WriteToText('regions.out')
    )
