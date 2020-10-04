import pandas as pd

def __f_clear(x, num):
	try:
		return x.split(',')[num]
	except Exception:
		return None

def clear_data(result_all_flatsList):
	df = pd.DataFrame(result_all_flatsList)
	df['count_rooms'] = df[0].apply(lambda x: __f_clear(x, 0))
	df['square'] = df[0].apply(lambda x: __f_clear(x, 1))
	df['count_floors'] = df[0].apply(lambda x: __f_clear(x,2))
	df.rename(columns={1: 'cost', 2: 'district', 3: 'flat_key'}, inplace = True)
	df.drop([0], axis = 1, inplace = True)
	return df