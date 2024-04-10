from sqlalchemy import create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database
from classes import CSVfile, TrainFunction
from bokeh.plotting import figure, output_notebook, show 


if __name__ == "__main__":

    #connect to database
    engine = create_engine("sqlite:///./data/function_data.db", echo=True, future=True)
    if not database_exists(engine.url):
        create_database(engine.url)

    conn = engine.connect()

    #read csv files
            
    ideal_csv = CSVfile("ideal.csv", "ideal_functions", 1)

    ideal_csv.save_to_db(conn, 'ideal_functions')

    train_csv = CSVfile("train.csv", "train_functions", 2)

    train_csv.save_to_db(conn, "train_functions")

    p = figure(title="log axis example", y_axis_type="log",
           background_fill_color="#fafafa")

    train_functions = []
    ideal_functions = []

    #Match all test data string by sting
    colors_ideal = ['#C60400','#C68100', '#C66000', '#C6B700']

    colors_train = ['#009CC6','#0063C6', '#00C6A8', '#5A00C6']

    for i in range (1,5):
        func_name = "y"+str(i)
        
        train_functions.append(TrainFunction(conn, func_name))

        p.line(x = train_functions[i-1].data_frame['x'], y = train_functions[i-1].data_frame[func_name], legend_label=f"trainfunc {func_name}", line_color=colors_train[i-1]) 
        
        picked_ideal_func = train_functions[i-1].choose_from_ideals(conn, 'ideal_functions')

        p.line(x = picked_ideal_func.data_frame["x"], y = picked_ideal_func.data_frame[picked_ideal_func.name], legend_label=f"idealfunc {picked_ideal_func.name}", line_color=colors_ideal[i-1]) 

        ideal_functions.append(picked_ideal_func)

    test_csv = CSVfile("test.csv", "test_data", 3)

    p.scatter(x=test_csv.data_frame['x'], y = test_csv.data_frame['y'], legend_label=f"test data", line_color="#20101D")

    test_csv.assign_test_data(conn, "test_data", ideal_functions)

    print(f'All possible test data is assigned to ideal functions. Results can be found in table "test_data"')

    p.legend.location = "top_left"

    show(p)
