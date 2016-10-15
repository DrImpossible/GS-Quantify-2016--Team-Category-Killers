import pandas as pd
import warnings
import cPickle
from datetime import datetime
warnings.filterwarnings("ignore")

home_folder = '/media/ameya/Research/gs_quantify/'
input1_filename = home_folder+'Data/metadata'
input2_filename = home_folder+'Data/dataset'
output_filepath = './Data/stocks/'

def saveData(data,filename):
    filehandler = open(filename+'.pkl','wb')
    cPickle.dump(data,filehandler)

def loadData(filename):
	file = open(filename+'.pkl','rb')
	data = cPickle.load(file)
	return data

def checkDataframesanity(df):
	print(df.ix[0])

def mergeDataframes(df1, df2):
	data = pd.merge(df2,df1, left_on='isin', right_on='isin', how='left')
	return data

def sampledata(df):
	for stock in pd.unique(df['isin_'].values.ravel()):
		print('Analyzing '+stock+' ...')
		data = df.loc[df['isin'] == stock]
		#print(data)
		saveData(data,output_filepath+stock+'_data')
		print('Data analyzed and stored!')

def sum_price(grp):
	return grp['price'].sum()
def sum_volume(grp):
	return grp['volume'].sum()
def avg_volume(grp):
	return (grp['volume'].sum()) / (grp['timeofday'].count()) 
def add_additional_columns(df):
	for stock in pd.unique(df['isin'].values.ravel()):
		data = df.loc[df['isin'] == stock]
		columns = list(df.columns[7:10]) + list(['timeofday', 'price', 'volume'])
		data = data.dropna(subset=columns)
		if len(data.index) == 0:
			continue;
		print 'Feature Size ' + str(data.shape)
		assert(data.shape[1] == 71)
		data = pd.merge(data, data.groupby(columns).size().reset_index(name='transactions'), on=columns)
		data = pd.merge(data, data.groupby(columns).apply(sum_price).reset_index(name='aggregrate_price'), on=columns)
		data = pd.merge(data, data.groupby(columns).apply(sum_volume).reset_index(name='aggregrate_volume'), on=columns)
		data = pd.merge(data, data.groupby(columns).apply(avg_volume).reset_index(name='average_volume'), on=columns)
		data['interval'] = list(range(len(data.index)))
		print (data[columns + list(['transactions', 'interval', 'aggregrate_volume', 'average_volume'])])
		saveData(data, output_filepath+stock+'_data_additional')
		assert(data.shape[1] == 76)		
		print 'Feature Size ' + str(data.shape)
		print('Data printed!')

if __name__ == '__main__':
	print('Loading data...')
	df = loadData(input1_filename)
	df2 = loadData(input2_filename)
	print('Data loaded!')
	#checkDataframesanity(df)
	#checkDataframesanity(df2)
	data = mergeDataframes(df,df2)
	#print(data)
	checkDataframesanity(data)
	saveData(data,'./Data/input_data')		
	#sampledata(data)
	add_additional_columns(data)
