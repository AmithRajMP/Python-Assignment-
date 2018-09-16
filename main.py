"""
Module to read data from CSV files and HTML file
to populate an SQL database

ITEC649 2018
"""

import csv
import sqlite3
import bs4
from database import DATABASE_NAME


# define your functions to read data here

def csvp_reader(file_obj){
    reader = csv.reader(file_obj)
    for row in reader:
        print(" ".join(row))
}

def csvc_reader(file_obj1){
    reader = csv.reader(file_obj)
    for row in reader:
        print(" ".join(row))
}

def parse_positions(file_obj2){

}

if __name__=='__main__':
    db = sqlite3.connect(DATABASE_NAME)

    # Add your 'main' code here to call your functions
csvp_path = "people.csv"
    with open(csvp_path, "rb") as f_obj:
        csvp_reader(f_obj)

csvc_path = "company.csv"
    with open(csvc_path, "rb") as f_obj:
        csvc_reader(f_obj1)