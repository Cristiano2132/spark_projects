# Imports
import pyspark.pandas as ps
import numpy as np

technologies = ({
    'Fee': [20000, 25000, 30000, 22000, np.NaN],
    'Discount': [1000, 2500, 1500, 1200, 3000]
})
# Create a DataFrame
psdf = ps.DataFrame(technologies)
print(psdf)


def add(data):
    return data[0] + data[1]


addDF = psdf.apply(add, axis=1)
print(addDF)