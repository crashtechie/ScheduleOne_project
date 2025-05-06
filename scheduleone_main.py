import pandas as pd
import os
import sqlite3
import datetime

# create timing wrappers for functions
def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = pd.Timestamp.now()
        result = func(*args, **kwargs)
        end_time = pd.Timestamp.now()
        print(f"Function {func.__name__} took {end_time - start_time} to execute.")
        return result
    return wrapper

# create a wrapper that catches exceptions and writes them to a log file in the output logs directory
# entry in log file should be in the format {current date/time formated "mm/dd/yyyy_hh:mm:ss"}_{function name}: {error message}
# if log file with the name error_log_{current date}.txt does not exist, create it
# if it does exist, append to it
def log_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # create the output/logs directory if it does not exist
            if not os.path.exists('output/logs'):
                os.makedirs('output/logs')
            
            # get the current date and time
            current_time = datetime.datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
            
            # create the log file name
            log_file_name = f"output/logs/error_log_{datetime.datetime.now().strftime('%m-%d-%Y')}.txt"
            
            # write the error message to the log file
            with open(log_file_name, 'a') as f:
                f.write(f"{current_time} {func.__name__}: {str(e)}\n")
            
            raise e  # re-raise the exception after logging it
    return wrapper

@log_error
#@timeit
def main():
    # prepare output directories
    if not os.path.exists('output'):
        os.makedirs('output')
        
    if not os.path.exists('output/level_0_drugs'):
        os.makedirs('output/level_0_drugs')

    if not os.path.exists('output/level_1_drugs'):
        os.makedirs('output/level_1_drugs')
    
    create_sqlite_db()
    create_base_drugs_table()
    create_level_1_drugs_table()
    
    drugs_df = pd.read_csv('data/combinations_data.csv')
    
    drugs_df = drugs_df.rename(columns={'base_item': 'base_drug', 'addition_item': 'item_name', 'name': 'drug_name', 'base_value': 'value', 'base_addiction': 'addiction_level'})
    drugs_df['drug_name'] = drugs_df['drug_name'].astype(str)
    drugs_df['level'] = drugs_df['level'].astype(int)
    drugs_df['base_drug'] = drugs_df['base_drug'].astype(str)
    drugs_df['item_name'] = drugs_df['item_name'].astype(str)
    drugs_df['effects'] = drugs_df['effects'].astype(str)
    drugs_df['value'] = drugs_df['value'].astype(int)
    drugs_df['addiction_level'] = drugs_df['addiction_level'].astype(float)

    drugs_df.set_index('drug_name', inplace=True)
    
    base_drugs = drugs_df[drugs_df['level'] == 0]
    base_drugs = base_drugs[['effects', 'value', 'addiction_level']]
    #base_drugs.to_csv('output/level_0_drugs/base_drugs.csv', index=True)
    insert_base_drugs_df(base_drugs)
    
    level_1_drugs = drugs_df[drugs_df['level'] == 1]
    #print(level_1_drugs[level_1_drugs['base_drug'] == 'test_drug'])
    level_1_drugs = level_1_drugs[['item_name','base_drug','effects', 'value', 'addiction_level']]
    level_1_drugs['value_increase'] = level_1_drugs['value'] - base_drugs.loc[level_1_drugs['base_drug'], 'value'].values
    level_1_drugs['addiction_increase'] = round(level_1_drugs['addiction_level'] - base_drugs.loc[level_1_drugs['base_drug'], 'addiction_level'].values,2)
    level_1_drugs = level_1_drugs[['base_drug', 'item_name', 'effects', 'value_increase', 'addiction_increase']]
    
    #level_1_drugs.to_csv('output/level_1_drugs/level_1_drugs.csv', index=True)
    insert_level_1_drug_df(level_1_drugs)
    
    # get the item that has the highest value increase for og_kush
    og_kush = level_1_drugs[level_1_drugs['base_drug'] == 'og_kush']
    og_kush_max = og_kush.loc[og_kush['value_increase'].idxmax()]
    og_kush_max = og_kush_max[['item_name', 'value_increase', 'addiction_increase']]
    print(f'Item with the highest value increase for og_kush: {og_kush_max['item_name']}')
    
    # get the item that has the higest vaalue increase for sour_diesel
    sour_diesel = level_1_drugs[level_1_drugs['base_drug'] == 'sour_diesel']
    sour_diesel_max = sour_diesel.loc[sour_diesel['value_increase'].idxmax()]
    sour_diesel_max = sour_diesel_max[['item_name', 'value_increase', 'addiction_increase']]
    print(f'Item with the highest value increase for sour_diesel: {sour_diesel_max['item_name']}')
    
    # get the item that has the highest value increase for green_crack
    green_crack = level_1_drugs[level_1_drugs['base_drug'] == 'green_crack']
    green_crack_max = green_crack.loc[green_crack['value_increase'].idxmax()]
    green_crack_max = green_crack_max[['item_name', 'value_increase', 'addiction_increase']]
    print(f'Item with the highest value increase for green_crack: {green_crack_max['item_name']}')
    
    # get the item that has the highest value increase for granddaddy_purple
    granddaddy_purple = level_1_drugs[level_1_drugs['base_drug'] == 'granddaddy_purple']
    granddaddy_purple_max = granddaddy_purple.loc[granddaddy_purple['value_increase'].idxmax()]
    granddaddy_purple_max = granddaddy_purple_max[['item_name', 'value_increase', 'addiction_increase']]
    print(f'Item with the highest value increase for granddaddy_purple: {granddaddy_purple_max['item_name']}')

def create_sqlite_db():
    if not os.path.exists('output/database'):
        os.makedirs('output/database')
    
    try:
        #make a variable called sqlquery that gets the database version
        sqlquery = "SELECT sqlite_version();"
        
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        cursor.execute(sqlquery)
        data = cursor.fetchall()
        print("SQLite version: ", data)
    except sqlite3.Error as error:
        return error
    finally:
        if conn:
            conn.close()
    
def create_base_drugs_table():
    try:
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        
        # create the base drugs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS base_drugs (
                drug_name TEXT PRIMARY KEY,
                effects TEXT,
                value INTEGER,
                addiction_level REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
    except sqlite3.Error as error:
        return error
    finally:
        if conn:
            conn.close()

# create a function to insert base_drugs dataframe object if it does not exist into the base_drugs table
def insert_base_drugs_df(drugs_df):
    try:
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        
        # insert the base drugs data into the table
        for index, row in drugs_df.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO base_drugs (drug_name, effects, value, addiction_level, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (index, row['effects'], row['value'], row['addiction_level'], datetime.datetime.now(), datetime.datetime.now()))
        
        conn.commit()
    except sqlite3.Error as error:
        conn.rollback()
        print("Error while inserting data into base_drugs table: ", error)
        # log the error to a file
        if not os.path.exists('output/logs'):
            os.makedirs('output/logs')
        log_file_name = f"output/logs/db_error_log_{datetime.datetime.now().strftime('%m-%d-%Y')}.txt"
        with open(log_file_name, 'a') as f:
            f.write(f"{datetime.datetime.now().strftime('%m/%d/%Y_%H:%M:%S')} insert_base_drugs_data: {str(error)}\n")
    finally:
        if conn:
            conn.close()

def remove_base_drug(drug_name):
    try:
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        
        # remove the base drug from the table
        cursor.execute('''
            DELETE FROM base_drugs WHERE drug_name = ?
        ''', (drug_name,))
        
        conn.commit()
    except sqlite3.Error as error:
        conn.rollback()
        print("Error while removing data from base_drugs table: ", error)
        # log the error to a file
        if not os.path.exists('output/logs'):
            os.makedirs('output/logs')
        log_file_name = f"output/logs/db_error_log_{datetime.datetime.now().strftime('%m-%d-%Y')}.txt"
        with open(log_file_name, 'a') as f:
            f.write(f"{datetime.datetime.now().strftime('%m/%d/%Y_%H:%M:%S')} insert_base_drugs_data: {str(error)}\n")
    finally:
        if conn:
            conn.close()

def update_base_drug_data(drug_name, effects, value, addiction_level):
    try:
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        
        # update the base drug data in the table
        cursor.execute('''
            UPDATE base_drugs
            SET effects = ?, value = ?, addiction_level = ?, updated_at = ?
            WHERE drug_name = ?
        ''', (effects, value, addiction_level, datetime.datetime.now(), drug_name))
        
        conn.commit()
    except sqlite3.Error as error:
        conn.rollback()
        print("Error while updating data on base_drugs table: ", error)
        # log the error to a file
        if not os.path.exists('output/logs'):
            os.makedirs('output/logs')
        log_file_name = f"output/logs/db_error_log_{datetime.datetime.now().strftime('%m-%d-%Y')}.txt"
        with open(log_file_name, 'a') as f:
            f.write(f"{datetime.datetime.now().strftime('%m/%d/%Y_%H:%M:%S')} insert_base_drugs_data: {str(error)}\n")
    finally:
        if conn:
            conn.close()

def create_level_1_drugs_table():
    try:
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        
        # create the level 1 drugs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS level_1_drugs (
                drug_name TEXT PRIMARY KEY,
                base_drug TEXT,
                item_name TEXT,
                effects TEXT,
                value_increase INTEGER,
                addiction_increase REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (base_drug) REFERENCES base_drugs(drug_name)
            )
        ''')
        conn.commit()
    except sqlite3.Error as error:
        return error
    finally:
        if conn:

            conn.close()

def insert_level_1_drug_df(drugs_df):
    try:
        conn = sqlite3.connect('output/database/scheduleone.db')
        cursor = conn.cursor()
        
        # insert the level 1 drugs data into the table
        for index, row in drugs_df.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO level_1_drugs (drug_name, base_drug, item_name, effects, value_increase, addiction_increase, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (index, row['base_drug'], row['item_name'], row['effects'], row['value_increase'], row['addiction_increase'], datetime.datetime.now(), datetime.datetime.now()))
        
        conn.commit()
    except sqlite3.Error as error:
        conn.rollback()
        print("Error while inserting data into level_1_drugs table: ", error)
        # log the error to a file
        if not os.path.exists('output/logs'):
            os.makedirs('output/logs')
        log_file_name = f"output/logs/db_error_log_{datetime.datetime.now().strftime('%m-%d-%Y')}.txt"
        with open(log_file_name, 'a') as f:
            f.write(f"{datetime.datetime.now().strftime('%m/%d/%Y_%H:%M:%S')} insert_base_drugs_data: {str(error)}\n")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()