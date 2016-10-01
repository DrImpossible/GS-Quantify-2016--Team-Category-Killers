import pandas as pd
import warnings
import cPickle
warnings.filterwarnings("ignore")

home_folder = '/home/ameya/gs_quantify/'

input1_filename = home_folder+'Data/metadata.csv'
input2_filename = home_folder+'Data/dataset.csv'
output1_filename = home_folder+'Data/metadata'
output2_filename = home_folder+'Data/dataset'

def inputFile(filename):
    fileformat = filename.split('.')[1]
    filename = filename.split('.')[0]
    print(filename+'.'+fileformat)

    if fileformat=='csv':
        df = pd.read_csv(filename+'.'+fileformat, parse_dates= True, infer_datetime_format = True)
    return df

def saveData(data,filename):
    filehandler = open(filename+'.pkl','wb')
    cPickle.dump(data,filehandler)

def loadData(filename):
	file = open(filename+'.pkl','rb')
	data = cPickle.load(file)
	return data

def checkDataframesanity(df):
	print(df.ix[0])

def finduniqueDataframe(df):
	for column in df.columns:
		#print('New instance:')
		#print(isinstance(df[column][0], pd.DatetimeIndex))
		print(column+' Unique values : '+str(df[column].nunique()))
		print(column+' Total values : '+str(len(df[column].values)))
		print('Example value : '+str(df[column][1]))

if __name__ == '__main__':
	df = inputFile(input1_filename)
	df2 = inputFile(input2_filename)
	checkDataframesanity(df)
	checkDataframesanity(df2)
	#saveData(df, output1_filename)
	#saveData(df2,output2_filename)
	finduniqueDataframe(df)
	finduniqueDataframe(df2)