import pandas as pd
import warnings
import cPickle
from datetime import datetime
import numpy as np
import random
from sklearn.cross_validation import train_test_split
from sklearn.feature_extraction import DictVectorizer
from sklearn.preprocessing import OneHotEncoder as OHE
from sklearn.preprocessing import MinMaxScaler as MMS
from sklearn.preprocessing import LabelEncoder as LE

warnings.filterwarnings("ignore")
datetimeColumns = ['issueDate','maturity','ratingAgency1EffectiveDate','ratingAgency2EffectiveDate']
specialColumn = ['date','time']
featureTypes = {}

home_folder = '/media/ameya/Research/gs_quantify/'
input1_filename = home_folder+'Data/metadata'
input2_filename = home_folder+'Data/dataset'
output_filepath = './Data/stocks/'
sequence_length = 30 #Keep it big enough so that Y_train and Y_test is different
numsamples = 100
featuresize = 76
labelmaker = LE()
vec = DictVectorizer()
encoder = OHE(sparse=False)


def saveData(data,filename):
    filehandler = open(filename+'.pkl','wb')
    cPickle.dump(data,filehandler)

def loadData(filename):
    file = open(filename+'.pkl','rb')
    data = cPickle.load(file)
    return data

def checkDataframesanity(df):
    print(df.ix[0])

def encode_onehot_main(df, cols):
    
    cols = [cols]
    vec.partial_fit(df[cols].to_dict(outtype='records'))
    vec_data = pd.DataFrame(vec.transform(df[cols].to_dict(outtype='records')).toarray())
    vec_data.columns = vec.get_feature_names()
    vec_data.index = df.index
    
    df = df.drop(cols, axis=1)
    df = df.join(vec_data)
    return df

def encode_onehot(df, cols):
    vec_data = pd.DataFrame(vec.transform(df[cols].to_dict(outtype='records')).toarray())
    vec_data.columns = vec.get_feature_names()
    vec_data.index = df.index
    
    df = df.drop(cols, axis=1)
    df = df.join(vec_data)
    return df

def checkNumerical(value):
	try:
   		val = float(value)
   		return True

	except ValueError:
		return False

def preprocess_main(df):
    for column in df.columns:
        if column in specialColumn or column in datetimeColumns:
            del df[column]
        elif checkNumerical(df[column][1]):
            print(column+' : Numerical')
            featureTypes[column] = 'Numerical'
            df[column] = (df[column] - df[column].mean()) / df[column].std()

        else:
            print(column+' : Categorical')
            featureTypes[column] = 'Categorical'
            #Label encoding for converting strings into labels
            df[column] = labelmaker.fit_transform(df[column])
            df = encode_onehot_main(df,column)
    print(df.shape)
    return df

def preprocess(df):
    for column in df.columns:
        if column in specialColumn or column in datetimeColumns:
            del df[column]
        elif checkNumerical(df[column][1]):
            print(column+' : Numerical')
            featureTypes[column] = 'Numerical'
            df[column] = (df[column] - df[column].mean()) / df[column].std()

        else:
            print(column+' : Categorical')
            featureTypes[column] = 'Categorical'
            #Label encoding for converting strings into labels
            df[column] = labelmaker.transform(df[column])
            df = encode_onehot(df,column)
    print(df.shape)
    return df


def pickSample(val, df):
    data = df.loc[df['interval'] == val]
    #print(data)
    #print(data.iloc[0])
    #print(data.shape)
    return data.iloc[random.randrange(0,data.shape[0])]

def sampledata(df):
    samples = np.zeros((sequence_length,featuresize))
    for stock in pd.unique(df['isin'].values.ravel()):
        print('Analyzing '+stock+' ...')
        data = loadData(output_filepath+stock+'_data_additional')
        data = preprocess(data)
        print(data.shape)
        intervals = pd.unique(data['interval'].values.ravel())
        print(intervals.shape)
        #Effectively we have 75% training and 25% testing available, just that the testing part is strictly after half of the data
        intervallen = intervals.shape[0]/2
        inttrain, inttest = train_test_split(intervals[intervallen:], test_size=0.5, random_state=42)
        train_intervals = np.concatenate([intervals[:intervallen], inttrain])
        test_intervals = inttest
        print(train_intervals.shape,test_intervals.shape)

        if(train_intervals.shape[0]<sequence_length):
            continue

        nodone = 0
        idx = np.arange(train_intervals.shape[0])
        while(nodone<numsamples):
            currsample = np.zeros(featuresize)
            nodone = nodone+1
            np.random.shuffle(idx)
            idx = sorted(idx[:sequence_length])
            for val in xrange(len(idx)):
                sample = pickSample(val, data)
                #print(currsample.shape,sample.shape)
                currsample = np.vstack((currsample,sample))
                #print(currsample.shape)
            currsample = np.delete(currsample,0,0)
            #print(samples.shape,currsample.shape)
            samples = np.dstack((samples,currsample))
            #print(samples.shape)
    samples = np.delete(currsample,a[:,:,0])
    return samples

def splitinpout(samples):
    #Assuming we reorder the df such that the outputs are always at the end.
    #X = samples(:,:,:val)
    #Y = samples(:,:,val:)
    pass


if __name__ == '__main__':
    df = loadData('./Data/input_data')
    preprocess_main(df)
    samples = sampledata(df)
    saveData(samples,'./Data/training')







        

