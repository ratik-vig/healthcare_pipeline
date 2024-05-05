
from sklearn.model_selection import train_test_split
import numpy as np
from sklearn.metrics import f1_score
from sklearn.utils.class_weight import compute_class_weight
from imblearn.over_sampling import SMOTE
import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
import tensorflow as tf
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt



class LogisticRegression:
    def __init__(self):
            self.model = Sequential()
            self.model.add(Dense(1, input_dim=24, activation='sigmoid'))

class Model1:
    def __init__(self):
        self.model = Sequential()
        self.model.add(Dense(64, input_dim=24, activation='relu'))
        self.model.add(Dense(32, activation='relu'))
        self.model.add(Dense(32, activation='relu'))
        self.model.add(Dense(16, activation='relu'))
        self.model.add(Dense(1, activation='sigmoid'))

class Model2:
    def __init__(self):
        self.model = Sequential()
        self.model.add(Dense(128, input_dim=24, activation='relu'))
        self.model.add(Dense(64, activation='relu'))
        self.model.add(Dense(32, activation='relu'))
        self.model.add(Dense(16, activation='relu'))
        self.model.add(Dense(1, activation='sigmoid'))

class ML:

    def train(self, df):
        smote = SMOTE(random_state=42)
        x, y = smote.fit_resample(df.drop('DIABETE3', axis=1), df['DIABETE3'])

        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=7)
        model  = Model2()

        with tf.device(device_name='/CPU:0'):
            model.model.compile(loss='binary_crossentropy', optimizer=keras.optimizers.Adam(learning_rate=0.001), metrics=['accuracy'])

            history = model.model.fit(X_train, y_train, validation_split=0.1, epochs=50, batch_size=64)

            test_loss, test_accuracy = model.model.evaluate(X_test, y_test)
            print(f"Test accuracy: {test_accuracy * 100:.2f}%")

            plt.figure(figsize=(12, 6))
            plt.subplot(1, 2, 1)
            plt.plot(history.history['loss'], label='Training Loss')
            plt.plot(history.history['val_loss'], label='Validation Loss')
            plt.title('Training and Validation Loss')
            plt.xlabel('Epoch')
            plt.ylabel('Loss')
            plt.legend()

            plt.subplot(1, 2, 2)
            plt.plot(history.history['accuracy'], label='Training Accuracy')
            plt.plot(history.history['val_accuracy'], label='Validation Accuracy')
            plt.title('Training and Validation Accuracy')
            plt.xlabel('Epoch')
            plt.ylabel('Accuracy')
            plt.legend()

            plt.savefig('training_validation_metrics_dropout.png')

            plt.clf()
