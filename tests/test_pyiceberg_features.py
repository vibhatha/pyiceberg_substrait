from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

iceberg_catalog = load_catalog('default')
print(iceberg_catalog.list_namespaces())