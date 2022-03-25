
"""
pip3 install pandas pandas-gbq fsspec gcsfs
"""

import pandas as pd
df = pd.read_csv('gs://nts-staging-for-bq-upload/individuals.csv')

# To convert all object columns to strings (needed to upload to BQ, which has same formatting constraints as writing to parquet)
for i in range(len(df.dtypes)):
	if df.dtypes[i] == 'object':
		df.iloc[:,i] = df.iloc[:,i].astype('str')

id = 'dft-dst-prt-connectivitymetric'
df.to_gbq('nts.individuals', id)
