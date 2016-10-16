import pandas as pd
import warnings
import cPickle
from datetime import datetime
warnings.filterwarnings("ignore")

home_folder = '/media/ameya/Research/gs_quantify/'

input1_filename = home_folder+'Data/metadata.csv'
input2_filename = home_folder+'Data/dataset.csv'
output1_filename = home_folder+'Data/metadata'
output2_filename = home_folder+'Data/dataset'
datetimeColumns = ['issueDate','maturity','ratingAgency1EffectiveDate','ratingAgency2EffectiveDate']
specialColumn = ['date','time']
featureTypes = {}

def parsedatetime(time):
	try:
		date_object = datetime.strptime(time,'%a %d%b%y %I:%M:%S.000 %p')
	except:
		print(time)
		date_object = datetime.strptime(time,'%a %d%b%y %I:%M:%S.000 ')

	return date_object

def parsedate(date):
	date_object = datetime.strptime(date,'%d%b%Y')
	return date_object

def inputFile(filename):
    fileformat = filename.split('.')[1]
    filename = filename.split('.')[0]
    print(filename+'.'+fileformat)

    if fileformat=='csv':
        #df = pd.read_csv(filename+'.'+fileformat, parse_dates= True, infer_datetime_format = True)
        df = pd.read_csv(filename+'.'+fileformat)
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

def checkNumerical(value):
	try:
   		val = float(value)
   		return True

	except ValueError:
		return False

def normalizeFeatures(df, featureTypes):
	for column in df.columns:
		if column in datetimeColumns:
			print(column+' : Datetime')
			#print(df[column][1:5])
			df[column] = pd.to_datetime(df[column],infer_datetime_format=True)
			df[column+'_year'] = pd.DatetimeIndex(df[column]).year
			df[column+'_month'] = pd.DatetimeIndex(df[column]).month
			df[column+'_day'] = pd.DatetimeIndex(df[column]).day
			df[column+'_time'] = (pd.DatetimeIndex(df[column]).year-2000)*365 + pd.DatetimeIndex(df[column]).month*30 + pd.DatetimeIndex(df[column]).day
			df[column+'_dayofweek'] = pd.DatetimeIndex(df[column]).weekday
			df[column+'_dayofyear'] = pd.DatetimeIndex(df[column]).dayofyear
			featureTypes[column] = 'Datetime'

		if column in specialColumn:
			print(column+' : SpecialColumn')
			if column == 'date':
				for idx in xrange(len(df[column].values)):
					df[column][idx] = parsedate(df[column][idx])

				df[column] = pd.to_datetime(df[column],infer_datetime_format=True)
				df[column+'_year'] = pd.DatetimeIndex(df[column]).year
				df[column+'_month'] = pd.DatetimeIndex(df[column]).month
				df[column+'_day'] = pd.DatetimeIndex(df[column]).day
				df[column+'_dayofweek'] = pd.DatetimeIndex(df[column]).weekday
				df[column+'_dayofyear'] = pd.DatetimeIndex(df[column]).dayofyear
				df[column+'_time'] =  (pd.DatetimeIndex(df[column]).year-2000)*365 + pd.DatetimeIndex(df[column]).month*30 + pd.DatetimeIndex(df[column]).day
				featureTypes[column] = 'SpecialColumn'

			if column == 'time':
				for idx in xrange(len(df[column].values)):
					df[column][idx] = parsedatetime(df[column][idx])

				df[column] = pd.to_datetime(df[column],infer_datetime_format=True)
				df[column+'_year'] = pd.DatetimeIndex(df[column]).year
				df[column+'_month'] = pd.DatetimeIndex(df[column]).month
				df[column+'_day'] = pd.DatetimeIndex(df[column]).day
				df[column+'_dayofweek'] = pd.DatetimeIndex(df[column]).weekday
				df[column+'_dayofyear'] = pd.DatetimeIndex(df[column]).dayofyear
				df[column+'_hour'] = pd.DatetimeIndex(df[column]).hour
				df[column+'_minute'] = pd.DatetimeIndex(df[column]).minute
				df[column+'_second'] = pd.DatetimeIndex(df[column]).second
				df[column+'_timeonday'] = pd.DatetimeIndex(df[column]).second+60*pd.DatetimeIndex(df[column]).minute+60*60*pd.DatetimeIndex(df[column]).hour
				df[column+'_time'] =  (pd.DatetimeIndex(df[column]).year-2000)*365 + pd.DatetimeIndex(df[column]).month*30 + pd.DatetimeIndex(df[column]).day
				df[column+'_overalltime'] = df[column+'_time']*24*60*60 + df[column+'_timeonday']
				featureTypes[column] = 'SpecialColumn'

	for column in df.columns:
		if column not in datetimeColumns and column not in specialColumn and checkNumerical(df[column][1]):
   			print(column+' : Numerical')
   			featureTypes[column] = 'Numerical'
   			#df[column] = (df[column] - df[column].mean()) / df[column].std()
   		
   		elif column not in datetimeColumns and column not in specialColumn:
   			featureTypes[column] = 'Categorical'
			print(column+' : Categorical')

	return [df,featureTypes]
		
def finduniqueDataframe(df):
	for column in df.columns:
		#print('New instance:')
		#print(isinstance(df[column][0], pd.DatetimeIndex))
		if column not in datetimeColumns  and column not in specialColumn:
			print(column+' Unique values : '+str(df[column].nunique())+ ' out of '+str(len(df[column].values))+' values')
			#print(column+' Total values : '+str(len(df[column].values)))
			print('Example value : '+str(df[column][1]))
		else:
			print(column+' is a datetime column!')

if __name__ == '__main__':
	df = inputFile(input1_filename)
	df2 = inputFile(input2_filename)
	checkDataframesanity(df)
	checkDataframesanity(df2)
	
	[df, featureTypes] = normalizeFeatures(df, featureTypes)
	[df2, featureTypes] = normalizeFeatures(df2, featureTypes)
	df2 = df2.sort(['time'+'_overalltime','isin'], ascending=[True, True])
	saveData(df, output1_filename)
	saveData(df2,output2_filename)
	finduniqueDataframe(df)
	finduniqueDataframe(df2)
