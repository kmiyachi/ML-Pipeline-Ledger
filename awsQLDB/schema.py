LEDGER_NAME = "StockMarketPrediction"

ML_PIPELINE = {
    'Collection': ['GitHash', 'ScrapeTime', 'InputData', 'OutputHash'],
    'Training': ['GitHash', 'TrainTime', 'InputHash', 'Model'],
    'Prediction': ['GitHash', 'PredictionTime','InputHash', 'Model', 'OutputHash']
}


