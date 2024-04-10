

from sqlalchemy import inspect
import pandas as pd
import numpy as np
from math import sqrt

class CSVfile():
    def __init__(self, path, name, type=0):
        """parses csv-file into data_frame and check all the columns required for different type
        0 - unknown type
        1 - ideal functions csv
        2 - train functions csv
        3 - test data 
        if the type is unknow or file format is wrong throw exeption"""
        self.name = name
        self.type = type

        match self.type:
            case 0:
                self.data_frame = pd.read_csv(path)
                print(f"Object from unknow type file {self.name} succesfully created")
            case 1:
                data_frame = pd.read_csv(path)

                # Check if data_frame has all neededcolumns and all data is of right type
                validation_errors = 0
                if ("x" not in data_frame.columns):
                    validation_errors += 1
                if (data_frame.dtypes["x"] != float):
                    validation_errors += 1
                    print("not all values of type float")
                for i in range(1,50):
                    check_column = "y" + str(i)
                    if (check_column not in data_frame.columns):
                        validation_errors += 1
                    if (data_frame.dtypes[check_column] != float):
                        validation_errors += 1
                        print("not all values of type float")

                if (validation_errors == 0) :
                    self.data_frame = data_frame
                else: raise ValueError("All columns should be present and all values should be of type float") 

            case 2:
                data_frame = pd.read_csv(path)
                
                # Check if data_frame has all neededcolumns and all data is of right type
                validation_errors = 0
                if ("x" not in data_frame.columns):
                    validation_errors += 1
                if (data_frame.dtypes["x"] != float):
                    validation_errors += 1
                    print("not all values of type float")
                for i in range(1,4):
                    check_column = "y" + str(i)
                    if (check_column not in data_frame.columns):
                        validation_errors += 1
                    if (data_frame.dtypes[check_column] != float):
                        validation_errors += 1
                        print("not all values of type float")

                if (validation_errors == 0) :
                    self.data_frame = data_frame
                else: raise ValueError("All columns should be present and all values should be of type float") 

            case 3:
                data_frame = pd.read_csv(path)

                # Check if data_frame has all neededcolumns and all data is of right type
                validation_errors = 0
                if ("x" not in data_frame.columns):
                    validation_errors += 1
                if (data_frame.dtypes["x"] != float):
                    validation_errors += 1
                    print("not all values of type float")
                if ("y" not in data_frame.columns):
                    validation_errors += 1
                if (data_frame.dtypes["y"] != float):
                    validation_errors += 1
                    print("not all values of type float")
                if (validation_errors == 0) :
                    self.data_frame = data_frame
                else: raise ValueError("All columns should be present and all values should be of type float") 


    def save_to_db(self, conn, table_name):
        """if no such table it save, unless run exception
        check if the table to save id for right type of file"""

        match self.type:
            case 1:
                if table_name in ["train_functions", "test_data"]:
                    raise AttributeError("You are trying to save file to wrong type of table") 
            case 2:
                if table_name in ["ideal_functions", "test_data"]:
                    raise AttributeError("You are trying to save file to wrong type of table") 
            case 3:
                if table_name in ["ideal_functions", "train_functions"]:
                    raise AttributeError("You are trying to save file to wrong type of table") 

        self.data_frame.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

    def assign_test_data(self, conn, table_name, ideal_functions):
        '''string by string check testData to four ideal functions,
        where ideal_functions is an array of IdealFuntion'''

        if (self.type != 3):
            raise AttributeError("this function works only with type 3") 
        
        self.data_frame['delta_Y'] = None
        self.data_frame['No_of_ideal_func'] = None

        for index, row in self.data_frame.iterrows():
            for ideal_function in ideal_functions:
                yRow = ideal_function.data_frame.loc[ideal_function.data_frame['x'] == row['x']]
                dev = sqrt(pow((row['y'] - yRow[ideal_function.name].values),2))
                maxDev = sqrt(ideal_function.max_deviation)
                if (dev < maxDev*sqrt(2)):
                    self.data_frame._set_value(index,'delta_Y', dev)
                    self.data_frame._set_value(index,'No_of_ideal_func', ideal_function.name)
        
        self.data_frame.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

class Function():
    def __init__(self, name):
        self.data_frame = pd.DataFrame()
        self.name = name

    def count_deviation (self, function):
        frame_for_count = self.data_frame.merge(function.data_frame, left_on='x', right_on='x')
        sum_deviation = 0
        max_deviation = 0

        for index, row in frame_for_count.iterrows():
            deviation = pow((row.iloc[1] - row.iloc[2]),2)
            sum_deviation += deviation
            if (max_deviation < deviation):
                max_deviation = deviation
        return sum_deviation, max_deviation


class IdealFunction(Function):
    def __init__(self, conn, name):
        super().__init__(name)
        self.data_frame = pd.read_sql(sql=(f"SELECT x, {name} FROM ideal_functions"),con=conn)        

class TrainFunction(Function):
    def __init__(self, conn, name):
        super().__init__(name)
        self.data_frame = pd.read_sql(sql=(f"SELECT x, {name} FROM train_functions"),con=conn)

    def choose_from_ideals(self, conn, table_name):
        #for each column create an function and use super.count_deviation to find minimal.
        
        inspector = inspect(conn)
        funcs = inspector.get_columns(table_name)

        bestMatch = funcs[1]['name'] #First value is x, so we assign first function
        bestMatchFunction = IdealFunction(conn, funcs[1]['name'])
        minimalSum_deviation, max_deviation = bestMatchFunction.count_deviation(self)

        for j in range(1 , len(funcs)):
            idealFunction = IdealFunction(conn, funcs[j]['name'])
            sum_deviation, maxFuncDeviation = idealFunction.count_deviation(self)
            if (sum_deviation < minimalSum_deviation):
                bestMatch = funcs[j]['name']
                minimalSum_deviation = sum_deviation
                bestMatchFunction = idealFunction
                bestMatchFunction.max_deviation = maxFuncDeviation
        return bestMatchFunction
