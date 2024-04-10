from sqlalchemy import create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database
from classes import CSVfile, TrainFunction

if __name__ == "__main__":
    
    engine = create_engine("sqlite:///./data/function_data.db", echo=True, future=True)
    if not database_exists(engine.url):
        create_database(engine.url)

    conn = engine.connect()
            
    ideal_csv = CSVfile("ideal.csv", "ideal_functions", 1)

    ideal_csv.save_to_db(conn, 'ideal_functions')

    train_csv = CSVfile("train.csv", "train_functions", 2)

    train_csv.save_to_db(conn, "train_functions")

    train_functions = []
    ideal_functions = []

    for i in range (1,4):
        func_name = "y"+str(i)
        train_functions.append(TrainFunction(conn, func_name))
        ideal_functions.append(train_functions[i-1].choose_from_ideals(conn, 'ideal_functions'))

    test_csv = CSVfile("test.csv", "test_data", 3)

    test_csv.assign_test_data(conn, "test_data", ideal_functions)

    print(f'All possible test data is assigned to ideal functions. Results can be found in table "test_data"')
