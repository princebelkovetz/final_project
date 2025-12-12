import pandas as pd
import logging
import json
import os
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

# --- Load model ---
MODEL_PATH = "/app/model/catboost_airline_satisfaction.cbm"
THRESHOLD_PATH = "/app/model/optimal_threshold.json"  # optional but recommended

model = CatBoostClassifier()
model.load_model(MODEL_PATH)
logger.info('Pretrained CatBoost model loaded successfully from %s', MODEL_PATH)

OPTIMAL_THRESHOLD = 0.5

def make_pred(dt: pd.DataFrame, source_info: str = "kafka") -> pd.DataFrame:
    logger.info('Computing prediction probabilities...')
    proba = model.predict_proba(dt)[:, 1]
    pred = (proba >= OPTIMAL_THRESHOLD).astype(int)

    submission = pd.DataFrame({
        'score': proba,
        'satisfaction_flag': pred
    })
    logger.info(f'Prediction complete for data from {source_info} (threshold={OPTIMAL_THRESHOLD:.3f})')
    return submission


def get_prediction_proba(dt: pd.DataFrame) -> pd.Series:
    return pd.Series(model.predict_proba(dt)[:, 1], index=dt.index)


def get_top_features(n: int = 5) -> dict:
    importances = model.get_feature_importance()
    names = model.feature_names_
    top = sorted(zip(names, importances), key=lambda x: x[1], reverse=True)[:n]
    return dict(top)