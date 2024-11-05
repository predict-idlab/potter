# Import necessary libraries
import numpy as np
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from semantic import dataloader, featuretransformer, modelbuilder, Pipeline


# Function to load the dataset
@dataloader(output=['X_train', 'X_test', 'y_train', 'y_test'])
def load_dataset():
    iris = load_iris()
    X = iris.data
    y = iris.target
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test
    

# Function to create features and preprocess data
@featuretransformer(input=['X_train'], output=['X_train_scaled'])
def create_features(X_train):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)
    print(X_scaled)
    return X_scaled


# Function to train the model
@modelbuilder(input=['X_train_scaled', 'y_train'])
def train_model(X_train_scaled, y_train):
    classifier = LogisticRegression()
    classifier.fit(X_train_scaled, y_train)
    return classifier



def main():
    p = Pipeline()
    p.search()
    model = p.execute()
    _, X_test, _, y_test = load_dataset()
    X_test_scaled, _ = p.transform({'X_train':X_test, 'y_test':y_test})
    predictions = model.predict(X_test_scaled)

    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy:", accuracy)
    print(classification_report(y_test, predictions))


if __name__ == "__main__":
    main()
