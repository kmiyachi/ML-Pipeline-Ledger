







LEDGER_NAME = "StockMarketPrediction"

ML_PIPELINE = {
    'Collection': ['GitHash', 'ScrapeTime', 'InputData', 'OutputHash'],
    'Training': ['GitHash', 'TrainTime', 'InputHash', 'Model'],
    'Prediction': ['GitHash', 'PredictionTime','InputHash', 'Model', 'OutputHash']
}

# LEDGER_NAME = "Week4Demo"

# BASIC_PIPELINE = ["Collection", "Training", "Prediction"]

# FIVE_STEP_PIPELINE = ['Collection', 'FeatureExtraction', 'Training', 'Validation', 'Prediction']

# C_INDEX  = ['GitHash', 'ScrapeTime', 'DataPath', 'OutputHash']
# T_INDEX  = ['GitHash', 'TrainTime', 'InputHash', 'Model']
# P_INDEX  = ['GitHash', 'PredictionTime','InputHash', 'Model', 'OutputHash']

# FE_INDEX = ['GitHash', 'ExtractionTime', 'InputHash', 'OutputHash']
# V_INDEX  = ['GitHash', 'ValidationTime', 'InputHash', 'OutputHash']

# BASIC_INDEX = [C_INDEX, T_INDEX, P_INDEX]