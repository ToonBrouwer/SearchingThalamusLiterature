import sys
print("Python:", sys.version)
try:
    import numpy
    print("numpy:", numpy.__version__)
except Exception as e:
    print("numpy FAIL:", e)
try:
    import pandas
    print("pandas:", pandas.__version__)
except Exception as e:
    print("pandas FAIL:", e)
