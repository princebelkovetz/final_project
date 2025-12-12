import pandas as pd
import os

DATA_PATH = "./data/train.csv" 

def load_train_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH)
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
    return df

def run_preproc(train_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
    df = input_df.copy()

    if 'id' in df.columns:
        df = df.drop(columns=['id'])

    cat_cols = ['Gender', 'Customer Type', 'Type of Travel', 'Class']
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    median_flight_dist = train_df['Flight Distance'].median()
    df['Total Delay'] = df['Departure Delay in Minutes'] + df['Arrival Delay in Minutes']
    df['Is Delayed'] = (df['Total Delay'] > 0).astype(int)
    df['Long Flight'] = (df['Flight Distance'] > median_flight_dist).astype(int)

    train_cols = [
        'Gender', 'Customer Type', 'Age', 'Type of Travel', 'Class',
        'Flight Distance', 'Inflight wifi service',
        'Departure/Arrival time convenient', 'Ease of Online booking',
        'Gate location', 'Food and drink', 'Online boarding', 'Seat comfort',
        'Inflight entertainment', 'On-board service', 'Leg room service',
        'Baggage handling', 'Checkin service', 'Inflight service',
        'Cleanliness', 'Departure Delay in Minutes',
        'Arrival Delay in Minutes', 'Total Delay', 'Is Delayed', 'Long Flight'
    ]
    
    for col in train_cols:
        if col not in df.columns:
            raise ValueError(f"Missing expected column: {col}")
    
    df = df[train_cols]

    return df