import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

def train_model():
    print("Loading dataset for model training...")
    # Replace 'dataset.csv' with your cleaned telemetry or feature CSV file
    df = pd.read_csv('dataset.csv')
    
    # Select relevant numerical feature columns (e.g., vibration, acoustic, or statistical metrics)
    # Adjust column names based on your actual dataset structure
    features = df.select_dtypes(include=['float64', 'int64']).columns
    X = df[features].fillna(0)

    print(f"Training Isolation Forest on {X.shape[0]} rows and {X.shape[1]} features...")
    # Initialize Isolation Forest (contamination represents the expected proportion of outliers/anomalies)
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)

    # Save the trained model artifact
    joblib.dump(model, 'isolation_forest_model.pkl')
    joblib.dump(list(features), 'model_features.pkl')
    print("Model successfully trained and saved as 'isolation_forest_model.pkl'!")

if __name__ == "__main__":
    train_model()