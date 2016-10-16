np.random.seed(1337)  #for reproducibility
from __future__ import print_function
import numpy as np
import cPickle
from sklearn.cross_validation import train_test_split
from keras.preprocessing import sequence
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.embeddings import Embedding
from keras.layers.recurrent import LSTM, GRU, SimpleRNN
from keras.layers.convolutional import Convolution1D, MaxPooling1D

inp_dim = 76
inp_histlen = 30
output_dim = 4
batch_size = 64
nb_epoch = 250

def saveData(data,filename):
    filehandler = open(filename+'.pkl','wb')
    cPickle.dump(data,filehandler)

def loadData(filename):
	file = open(filename+'.pkl','rb')
	data = cPickle.load(file)
	return data

def buildModel(inp_dim, inp_histlen, output_dim):
	model = Sequential()
	model.add(GRU(128, input_shape=(inp_dim,inp_histlen), return_sequences=True, consume_less = 'gpu', init='he_normal'))
	model.add(Dropout(0.2))
	model.add(GRU(128, consume_less = 'gpu', return_sequences=True, init='he_normal'))
	model.add(Dropout(0.2))
	model.add(GRU(output_dim, consume_less = 'gpu', return_sequences=True, init='he_normal'))
	model.compile(loss='mean_squared_error',
              optimizer='adam')

	return model

if __name__ == '__main__':
	print('Loading data...')
	X = loadData('X')
	Y = loadData('Y')

	X_train, y_train, X_test, y_test = train_test_split(X, Y, test_size=0.25, random_state=42)
	print('Data successfully loaded!')

	print('Building model')
	model = buildModel(inp_dim,inp_histlen,output_dim)

	model.fit(X_train, y_train, batch_size=batch_size, nb_epoch=nb_epoch,
          validation_data=(X_test, y_test))
