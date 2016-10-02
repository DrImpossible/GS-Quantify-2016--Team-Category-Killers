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
	for stock in pd.unique(df['isin'].values.ravel()):
		print('Analyzing '+stock+' ...')
		data = df.loc[df['isin'] == stock]
		#print(data)
		saveData(data,output_filepath+stock+'_data')
		print('Data analyzed and stored!')

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
