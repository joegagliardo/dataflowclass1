import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from Category import Category

# Define the DoFn for parsing a line from the categories.csv file
class CategoryCSVParseClass(beam.DoFn):
    """
    A DoFn that processes a single comma-separated string element,
    parses it into its respective parts, and yields a Category object.
    """
    def process(self, element: str):
        """
        Processes a single line from the CSV.

        Args:
            element: A string representing one line from the categories.csv file,
                     e.g., "1,Electronics,Gadgets and devices".

        Yields:
            A Category object.
        """
        try:
            # Split the element by comma to get individual fields
            # We expect exactly three parts: category_id, category_name, description
            category_id_str, category_name, description = element.split(',')

            # Convert category_id to an integer
            category_id = int(category_id_str)

            # Yield a new Category NamedTuple
            yield Category(category_id, category_name, description)
        except (ValueError, IndexError) as e:
            # If any parsing error occurs (e.g., non-integer ID, wrong number of columns)
            # send the original malformed element string to the 'error_rows' tagged output.
            print(f"Parsing failed for line: '{element}' - Error: {e}")
            yield TaggedOutput('error_rows', element)

