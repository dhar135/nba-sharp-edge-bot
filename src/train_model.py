# src/train_model.py
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, classification_report
import joblib
import os
from utils import logger, timer

@timer
def train_xgboost_model():
    data_path = "data/ml_dataset_pts.csv"
    if not os.path.exists(data_path):
        logger.error(f"[!] Dataset not found at {data_path}. Run prep_ml_data.py first.")
        return

    logger.info("[*] Loading massive ML dataset into memory...")
    df = pd.read_csv(data_path)

    # 1. Define our Features (X) and our Target (y)
    # We are asking the AI to predict 'Hit_Over' using ONLY these 5 columns
    features = ['Is_Home', 'Days_Rest', '15g_Median_PTS', '5g_Median_PTS', 'Edge_Pct']
    X = df[features]
    y = df['Hit_Over']

    # 2. Train/Test Split
    # We hide 20% of the data from the AI so we can test its true accuracy later
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logger.info(f"[*] Training XGBoost Classifier on {len(X_train)} historical bets...")
    
    # 3. Initialize and Train the Model
    # These hyperparameters restrict the AI from "overfitting" (memorizing the data)
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        random_state=42,
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)

    # 4. Evaluate the Model on the hidden 20%
    logger.info("[*] Testing AI on hidden data...")
    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)[:, 1] # The raw % chance of hitting the OVER

    # 5. The Results
    accuracy = accuracy_score(y_test, predictions)
    
    # How often are we right when the AI specifically says "YES, BET THE OVER"?
    precision = precision_score(y_test, predictions) 

    logger.info("\n=== AI PERFORMANCE REPORT ===")
    logger.info(f"Overall Accuracy:  {accuracy * 100:.2f}%")
    logger.info(f"OVER Precision:    {precision * 100:.2f}% (Win rate when AI says 'BET OVER')")
    
    # 6. Feature Importance (What does the AI care about most?)
    logger.info("\n=== FEATURE IMPORTANCE (What matters most?) ===")
    importances = model.feature_importances_
    for feature, imp in zip(features, importances):
        logger.info(f"{feature:<15}: {imp * 100:.1f}%")

    # 7. Save the Brain
    out_dir = "models"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    model_path = os.path.join(out_dir, "xgb_pts_model.pkl")
    joblib.dump(model, model_path)
    logger.info(f"\n[+] AI Brain successfully saved to {model_path}!")
    logger.info("[*] The Math Engine is now ready to be upgraded to an ML Probability Engine.")

if __name__ == "__main__":
    train_xgboost_model()