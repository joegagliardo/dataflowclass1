from typing import NamedTuple
import apache_beam as beam
# from beam.coders import RowCoder

class Category(NamedTuple):
    """
    Represents a category with its ID, name, and a brief description.
    """
    category_id: int
    category_name: str
    description: str

    def _as_csv(self):
        return f"{self.category_id},{self.category_name},{self.description}"

    def _as_json(self):
        return str(self._asdict())

beam.coders.registry.register_coder(Category, beam.coders.RowCoder)

