"""
Module to read data from CSV files and HTML file
to populate an SQL database

ITEC649 2018
"""
    
import csv
import sqlite3
from database import DATABASE_NAME
import sqlite3
from bs4 import BeautifulSoup as BS
"""Importing all the necessary modules like CSV, 
SQLite, Beautiful Soup.
"""

# define your functions to read data here
# connect to database and return object
def connect():
	try:
	    # Creates or opens a file called db with a SQLite3 DB
	    db = sqlite3.connect(DATABASE_NAME)
	    # Get a cursor object
	    cursor = db.cursor()
	    return db
    
	except Error as e:
		raise e

	return None
"""Function definition: Creates or opens a file called 'db' with a SQLite3 DB. 
Get a cursor object.
"""   

# function to parse csv files to convert into list of dictionary
def parse(filename):
	reader = csv.DictReader(open(filename))
	object_list = []
	for entry in reader:
		object_list.append(entry)
	return object_list
"""Function definition: To parse csv files to convert into list of dictionary. 
Opening a file with Dictreader, creating an empty object list and appending 
it with the dictreader entries.
Returning the populated object list at the end of the function.
"""

# function to insert user data
def insert_user(data, db):
	
	user_list = []
	for user in data:		
		user_list.append(
			(user['person_ID'],
			user['first'],
			user['middle'],
			user['last'],
			user['email'],
			user['phone'])
			)

	try:
		db.executemany('''INSERT INTO people(id, first_name, middle_name, last_name, email, phone)
                  VALUES(?,?,?,?,?,?)''', user_list)
		
	except sqlite3.IntegrityError:
		print('Duplicate Data found. Exiting')
		pass
	finally:
		db.commit()
		return user_list
"""Function definition: To insert user data. 
Creating an empty user list and appending 
it with the user details headings like name, email ID etc using a for loop 
and the parameter data.
Try to insert values into people table in the DB with the user_list data, if there are duplicate entries throw an error.
Commit changes to the databases and return the user list in the end.
"""

# function to insert company data
def insert_company(data, db):
	
	company_list = []
	for company in data:		
		company_list.append(
			(company['company'],
			company['url'],
			company['contact'],
			company['location']
			))


	try:
		db.executemany('''INSERT INTO companies(name, url, contact, location)
                  VALUES(?,?,?,?)''', company_list)
				
	except sqlite3.IntegrityError:
		print('Duplicate Data found. Exiting')
		pass

	finally:
		db.commit()
		return company_list
"""Function definition: To insert company data. 
Creating an empty company list and appending 
it with the company details headings like ID, URL, contact etc using a for loop 
and the parameter data.
Try to insert values into companies table in the DB with the company_list data, if there are duplicate entries throw an error.
Commit changes to the databases and return the company list in the end.
"""

# function to insert positions data
def insert_positions(data, db):

	try:
		db.executemany('''INSERT INTO positions(title, location, company)
                  VALUES(?,?,?)''', data)
				
	except sqlite3.IntegrityError:
		print('Duplicate Data found. Exiting')
		pass

	finally:
		db.commit()
"""Function definition: To insert positions data. 
Try to insert values into the positions table in the DB with the data like title, 
location,company etc, if there are duplicate entries throw an error.
Commit changes to the databases in the end.
"""

# Function to find user_id from user_name
def find_userid(people_data, last, first, middle=None):
	for user in people_data:
		if middle:
			if (user['last'] == last[:-1]) and (user['first'] == first) and (user['middle'] == middle):
				return user['person_ID']
		else:
			if (user['last'] == last[:-1]) and (user['first'] == first):
				return user['person_ID']
		
	return None
"""Function definition: To find user_id from user_name. 
If there is middle name, then trying to match all the three parts of the name : First, last and middle.
If it matches, return the user ID.
Else if there is no middle name, then trying to match only two parts of the name : First and last.
If it matches, return the user ID.
If both of the above mentioned conditions doesnt match then return nothing.
"""

# parse the index file
def parse_html(filename):

	with open(filename) as f:
		html = f.read()


	bs = BS(html, 'lxml')

	positions = bs.findAll('div', {'class': 'card'})

	job_data = []
	for job in positions:
		text = job.find('h5').string
		company_name = job.find('div', {'class': 'company'}).string
		# find the company id
		db = connect()
		for row in db.execute("SELECT id, location from companies WHERE name=?", (company_name,)):
			company_ID = row[0]
			location = row[1]

			location = (text, location, company_ID)
			job_data.append(location)

	
	return job_data
"""Function definition: To parse HTML file by opening the file in read mode. 
Making use of beautiful soup to identify the HTML file.
Finding all the positions through beautiful soup with findAll.
Create an empty job_data list and scrape text, company_name and location using strings.
Allocate company ID and location in zeroeth row and first row respectively.
Append the job data with position which consists of text, location and company ID
Returning the populated Job data list at the end of the function.
"""



if __name__=='__main__':
	# db = sqlite3.connect(DATABASE_NAME)
	db = connect()

	# Add your 'main' code here to call your functions

	# parse people.csv

	people_data = parse('people.csv')
"""
Parsing people.csv by calling people_data function.
"""
	# insert user data into the database
	insert_user(people_data, db)
"""
Inserting user data into Database by using People_data function and insert_user function.
"""
	# parse companies.csv
	company_data = parse('companies.csv')

"""
Parsing companies.csv by calling company_data function.
"""
	# find the contact from company data and replace with id of the contact from people_data
	companies = []
	for company in company_data:
		contact_name = company['contact'].split()
		
		if len(contact_name) == 3:
			company.update({'contact': find_userid(people_data, contact_name[0], contact_name[1], contact_name[2])})
		else:
			company.update({'contact': find_userid(people_data, contact_name[0], contact_name[1])})
"""
Create an empty companies list.
Split contact_name in company using 'split' function.
If the length of contact_name is 3, that is First name, middle name and last name then update the company data.
If the length of contact_name is not 3, that is First name and last name only then update the company data.
Find the contact from company data and replace with id of the contact from people_data.
"""

	# insert updated company data in database
	insert_company(company_data, db)
"""
Inserting company data into Database by using company_data function and insert_company function.
"""

	# parse the jobs page
	positions_data = parse_html('index.html')
"""
Parsing index.html by calling positions_data function where beautiful soup is used.
"""
	insert_positions(positions_data, db)
"""
Inserting positions into positions_data by using DB data.
"""
	# Generate the final CSV file
	output_file = open('output.csv', 'wb')
	writer = csv.writer(output_file)
	writer.writerow(('Company Name', 'Position Title', 'Location', 'Contact First Name', 'Contact Last Name', 'Contact Email',))
	for row in db.execute(''' SELECT companies.name, positions.title, positions.location, people.first_name, people.last_name, people.email FROM positions inner join companies, people on positions.company=companies.id and companies.contact=people.id '''):

		company_name = row[0]
		position_title = row[1]
		location = row[2]
		first_name = row[3]
		last_name = row[4]
		email = row[5]

		writer.writerow(row)
"""
The output file is defined to be opened as output.csv in write format.
Use csv.writer to write the output file.
Write the output files in rows with the respective headings like position title, location etc as instructed .
Use select to get the data from respective tables to write in the CSV file.
Define which rows should be occupied those respective headers.
Write the output.csv file using writerow function.
"""

	# close the connection to database
	db.close()
