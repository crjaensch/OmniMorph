import sys
print('Python version:', sys.version)

try:
    import pyarrow.avro as avro
    print('Avro module available:', dir(avro))
except ImportError as e:
    print('Avro import error:', e)
