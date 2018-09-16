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

# define your functions to read data here
# connect to database and return object
def connect():
	try:
	    # Creates or opens a file called mydb with a SQLite3 DB
	    db = sqlite3.connect(DATABASE_NAME)
	    # Get a cursor object
	    cursor = db.cursor()
	    return db
    
	except Error as e:
		raise e

	return None
    

# function to parse csv files to convert into list of dictionary
def parse(filename):
	reader = csv.DictReader(open(filename))
	object_list = []
	for entry in reader:
		object_list.append(entry)
	return object_list

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


# function to insert company data
def insert_positions(data, db):

	try:
		db.executemany('''INSERT INTO positions(title, location, company)
                  VALUES(?,?,?)''', data)
				
	except sqlite3.IntegrityError:
		print('Duplicate Data found. Exiting')
		pass

	finally:
		db.commit()


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




if __name__=='__main__':
	# db = sqlite3.connect(DATABASE_NAME)
	db = connect()

	# Add your 'main' code here to call your functions

	# parse people.csv
	people_data = parse('people.csv')

	# insert user data into the database
	insert_user(people_data, db)

	# parse companies.csv
	company_data = parse('companies.csv')

	# find the contact from company data and replace with id of the contact from people_data
	companies = []
	for company in company_data:
		contact_name = company['contact'].split()
		
		if len(contact_name) == 3:
			company.update({'contact': find_userid(people_data, contact_name[0], contact_name[1], contact_name[2])})
		else:
			company.update({'contact': find_userid(people_data, contact_name[0], contact_name[1])})


	# insert updated company data in database
	insert_company(company_data, db)


	# parse the jobs page
	positions_data = parse_html('index.html')

	insert_positions(positions_data, db)

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


	# close the connection to database
	db.close()
