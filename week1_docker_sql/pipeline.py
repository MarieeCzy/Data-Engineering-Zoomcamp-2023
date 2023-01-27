import sys
import pandas as pd

#commandline arguments
print(sys.argv)

#argument nr 0 jest nazwą pliku, więc uyjemy argumentu nr 1
day = sys.argv[1]

print(f'job finished successfully for day = {day}!') 