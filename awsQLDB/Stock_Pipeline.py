import os
import sys
from pandas_datareader import data as pdr
import pandas as pd
import fix_yahoo_finance as yf
import importlib
from create_ledger import *
from connect_to_ledger import *
from create_table import *
from create_index import * 
from insert_document import *
from schema import *
from config import *
import pandas as pd


sys.path.append("../MachineLearningStocks/")
from download_historical_prices import *
from parsing_keystats import *
from stock_prediction import *
from sklearn.ensemble import RandomForestClassifier
from utils import data_string_to_float, status_calc



# The percentage by which a stock has to beat the S&P500 to be considered a 'buy'
OUTPERFORMANCE = 10
def collectionData(dataPath, outHash):
    repo = git.Repo(search_parent_directories=True)
    commit_hash = repo.head.object.hexsha
    collection_data = [
        {
            'GitHash': commit_hash,
            'ScrapeTime': datetime.now(),
            'DataPath': dataPath,
            'outputHash': outHash
        }
    ]
    return collection_data

def trainingData(inputHash, model):
    repo = git.Repo(search_parent_directories=True)
    commit_hash = repo.head.object.hexsha
    training_data = [
        {
            'GitHash': commit_hash,
            'TrainTime': datetime.now(),
            'InputHash': inputHash,
            'Model': model
        }
    ]
    return training_data

def predictionData(inputHash, model, outHash):
    repo = git.Repo(search_parent_directories=True)
    commit_hash = repo.head.object.hexsha
    prediction_data = [
        {
            'GitHash': commit_hash,
            'InputHash': inputHash,
            'Model': model, 
            'PredictionTime': datetime.now(),
            'OutputHash': outHash
        }
    ]
    return prediction_data


def build_data_set():
    """
    Reads the keystats.csv file and prepares it for scikit-learn
    :return: X_train and y_train numpy arrays
    """
    training_data = pd.read_csv("keystats.csv", index_col="Date")
    training_data.dropna(axis=0, how="any", inplace=True)
    features = training_data.columns[6:]

    X_train = training_data[features].values
    # Generate the labels: '1' if a stock beats the S&P500 by more than 10%, else '0'.
    y_train = list(
        status_calc(
            training_data["SP500_p_change"],
            OUTPERFORMANCE,
        )
    )

    return X_train, y_train


def predict_stocks():
    X_train, y_train = build_data_set()
    # Remove the random_state parameter to generate actual predictions
    clf = RandomForestClassifier(n_estimators=100, random_state=0)
    clf.fit(X_train, y_train)

    # Now we get the actual data from which we want to generate predictions.
    data = pd.read_csv("forward_sample.csv", index_col="Date")
    data.dropna(axis=0, how="any", inplace=True)
    features = data.columns[6:]
    X_test = data[features].values
    z = data["Ticker"].values

    # Get the predicted tickers
    y_pred = clf.predict(X_test)
    if sum(y_pred) == 0:
        print("No stocks predicted!")
    else:
        invest_list = z[y_pred].tolist()
        print(
            f"{len(invest_list)} stocks predicted to outperform the S&P500 by more than {OUTPERFORMANCE}%:"
        )
        print(" ".join(invest_list))
        return invest_list



TICKER = "SPY"
START = "2003-08-01"
END = "2020-01-01"

COUT = "sp500_index.csv"

def data_collection(ticker, start, end, out_file):
    # build_sp500_dataset()
    #execute_insert("Demo4Ledger", "Collection", "Test", "sp500_index.csv")
    index_data = pdr.get_data_yahoo(ticker, start, end)
    collection_data = collectionData("Yahoo", out_file)
    dynamic_insert("Week4Demo", "Collection", collection_data)
    index_data.to_csv(out_file)
    return index_data

# def feature_extraction(): 
#     sp500_df, stock_df = preprocess_price_data()
#     parse_keystats(sp500_df, stock_df)
#     execute_insert("Demo4Ledger", "Training", "Hash(sp500_index.csv)", "Model1")

def training():
    sp500_df, stock_df = preprocess_price_data()
    df = parse_keystats(sp500_df, stock_df)
    X_train, y_train = build_data_set()
    # Remove the random_state parameter to generate actual predictions
    clf = RandomForestClassifier(n_estimators=100, random_state=0)
    clf.fit(X_train, y_train)
    dynamic_insert("Week4Demo", "Training", hash(df), hash(clf))
    return clf

def predict(model, test_data):
    prediction = predict_stocks(model, test_data)
    dynamic_insert("Week4Demo", "Prediction", hash(test_data), "Model1", hash(prediction))
    return prediction


if __name__ == "__main__":
    data = data_collection(TICKER, START, END, COUT)
    model = training()
    pred = predict(model, "./test_data.csv")