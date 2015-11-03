'''
Created on Nov 1, 2015

@author: MatthewRubashkin
'''
#import package to connect to mysql database
#requirements: pip install mysql-connector-python --allow-external mysql-connector-python
import mysql.connector
from mysql.connector import errorcode

#function to connect to mysql server with output to user verifying that connection was made
def get_connection(database):
    try:
      cnx = mysql.connector.connect(user = 'root',password ='',host='127.0.0.1',port='3306',database=database, buffered=True) #removed password for security
      #print('Successfully connected to database')
      return cnx
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
    else:
      print('closing connection')
      cnx.close()

#Create database and table function, also enables future ETL work, including combining name columns and informed lookups
def create_database_and_table(database,table_name):
    #create the layout/schema of the table
    schema= ("CREATE TABLE IF NOT EXISTS " + str(table_name) + " ("
    'ID BIGINT AUTO_INCREMENT NOT NULL,'
    'Physician_Profile_ID BIGINT,'
    'Physician_Full_Name VARCHAR(100),'
    'Recipient_State VARCHAR(2),'
    'Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name VARCHAR(100),'
    'Total_Amount_of_Payment_USDollars DECIMAL,'
    'PRIMARY KEY (ID))')
    
    #create the table and databse if it did not previously exist
    with get_connection(database) as conn:
        print('Creating database %s' % database)
        conn.query('CREATE DATABASE IF NOT EXISTS %s' % database)
        conn.query('USE %s' % database)
        conn.query('CREATE TABLE IF NOT EXISTS tbl ('+schema+')') #(id INT AUTO_INCREMENT PRIMARY KEY)')

#Import the raw data (CMS-Explorer), originally downloaded as csv then copied to EC2
def import_data_to_table(database,table_name,file_location):
    #commands for inserting into a csv, uses ignore to skip over columns to decrease database size
    mysql_query= ("LOAD DATA INFILE" +"'" +file_location + "'" 
    "into table " + table_name + 'fields terminated by ' + ","
    'optionally enclosed by' + '"' 
    "lines terminated by '\n'" 
    "ignore 1 lines ("
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    Physician_Profile_ID    "
    "    Physician_First_Name    "
    "    Physician_Middle_Name    "
    "    Physician_Last_Name    "
    "    Physician_Name_Suffix    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    Recipient_State    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    Total_Amount_of_Payment_USDollars    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore,    "
    "    @ignore     ")
    with get_connection(database) as conn:
        conn.execute(mysql_query)

#returns a list of companies that are listed in the database for use in making index template for flask with direct company lookup
#therefore decreasing risk of sql-injection attack
def get_company_names(database,table):
    mysql_query=('SELECT DISTINCT Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name AS companies '
    'FROM ' + table + ';')
    #print mysql_query
    #create empty array for adding companies to
    companies_list=[]
    #execute query with cursor to get information
    try:
        conn = get_connection(database)
        curA=conn.cursor()
        #curA.execute(mysql_query,multi=True)
        #print 'curA',curA
        # Iterate through the result of curA
        for result in curA.execute(mysql_query,multi=True):
            if result.with_rows:
                for line in result.fetchall():
                    #append the name of the business to the companies list, which requires to grab the first item in the array
                    #and convert from unicode to string
                    companies_list.append(str(line[0]))
            
    finally:
        #print 'complete'
        curA.close()
        
    #sort the list for clarity
    companies_list.sort()
    return companies_list

#function is used to generate which states are represented in the data
#therefore decreasing risk of sql-injection attack
def get_states(database,table):
    mysql_query=('SELECT DISTINCT Recipient_State AS state '
    'FROM ' + table + ';')
    #print mysql_query
    #create empty array for adding companies to
    states_list=[]
    #execute query with cursor to get information
    try:
        conn = get_connection(database)
        curA=conn.cursor()
        #curA.execute(mysql_query,multi=True)
        #print 'curA',curA
        # Iterate through the result of curA
        for result in curA.execute(mysql_query,multi=True):
            if result.with_rows:
                for line in result.fetchall():
                    #append the name of the business to the companies list, which requires to grab the first item in the array
                    #and convert from unicode to string
                    #print line
                    state=str(line[0])
                    if state != '':
                        states_list.append(str(line[0]))
                    #print line[0], len(states_list)
            
    finally:
        #print 'complete'
        curA.close()
        
    #dictionary for pulling state names
    states_dict = {'AK': 'Alaska','AL': 'Alabama','AR': 'Arkansas','AS': 'American Samoa','AZ': 'Arizona','CA': 'California','CO': 'Colorado','CT': 'Connecticut','DC': 'District of Columbia','DE': 'Delaware','FL': 'Florida','GA': 'Georgia','GU': 'Guam','HI': 'Hawaii','IA': 'Iowa','ID': 'Idaho','IL': 'Illinois','IN': 'Indiana','KS': 'Kansas','KY': 'Kentucky','LA': 'Louisiana','MA': 'Massachusetts','MD': 'Maryland','ME': 'Maine','MI': 'Michigan','MN': 'Minnesota','MO': 'Missouri','MP': 'Northern Mariana Islands','MS': 'Mississippi','MT': 'Montana','NA': 'National','NC': 'North Carolina','ND': 'North Dakota','NE': 'Nebraska','NH': 'New Hampshire','NJ': 'New Jersey','NM': 'New Mexico','NV': 'Nevada','NY': 'New York','OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania','PR': 'Puerto Rico','RI': 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota','TN': 'Tennessee','TX': 'Texas','UT': 'Utah','VA': 'Virginia','VI': 'Virgin Islands','VT': 'Vermont','WA': 'Washington','WI': 'Wisconsin','WV': 'West Virginia','WY': 'Wyoming'}
    
    #sort the list for clarity
    states_list.sort()
    #get the fullname of the state for later use on web UI
    states_list_fullname=[]
    for state in states_list:
        state_name=states_dict[state]
        states_list_fullname.append(state_name)
        
    #print states_list_fullname
    #return back both the state abbreviations and states list with fullname
    return states_list,states_list_fullname 
            
#query_database function is also found in flask app            
def query_database(database,table,state,company):
    #check what type of query is coming in, and change the response accordingly
    if state=='All' and company=='All':
        mysql_query_whole_country=("SELECT Physician_Full_Name AS Physician, "
        "SUM(Total_Amount_of_Payment_USDollars) AS Received_the_following_money, "
        "count(Total_Amount_of_Payment_USDollars) AS In_this_many_payments "
        "FROM " + str(table) + " WHERE Physician_Full_Name <> '   ' "
        "GROUP BY Physician_Profile_ID "
        "ORDER BY SUM(Total_Amount_of_Payment_USDollars) DESC "
        "LIMIT 20;")
        mysql_query=mysql_query_whole_country
    elif company=='All':
        mysql_query_specific_state=("    SELECT Physician_Full_Name AS Physician,     "
        "    SUM(Total_Amount_of_Payment_USDollars) AS Received_the_following_money,     "
        "    count(Total_Amount_of_Payment_USDollars) AS In_this_many_payments,    "
        "    Recipient_State AS In_the_state_of    "
        "    FROM " + str(table) + "    WHERE Physician_Full_Name <> '   '     "
        "    AND Recipient_State = '" + state + "'"
        "    GROUP BY Physician_Profile_ID, Recipient_State "
        "    ORDER BY SUM(Total_Amount_of_Payment_USDollars) DESC    "
        "    LIMIT 20;    ")
        mysql_query=mysql_query_specific_state
    elif state=='All':
        mysql_query_specific_company=("    SELECT Physician_Full_Name AS Physician,     "
        "    SUM(Total_Amount_of_Payment_USDollars) AS Received_the_following_money,     "
        "    count(Total_Amount_of_Payment_USDollars) AS In_this_many_payments,    "
        "    Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name AS From_this_company    "
        "    FROM " + str(table) + "    WHERE Physician_Full_Name <> '   '     "
        "    AND Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name = '" + company + "' "
        "    GROUP BY Physician_Profile_ID, Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name    "
        "    ORDER BY SUM(Total_Amount_of_Payment_USDollars) DESC    "
        "    LIMIT 20;    ")
        mysql_query=mysql_query_specific_company
    else:
        mysql_query_specific_state_company=("    SELECT Physician_Full_Name AS Physician,     "
        "    SUM(Total_Amount_of_Payment_USDollars) AS Received_the_following_money,     "
        "    count(Total_Amount_of_Payment_USDollars) AS In_this_many_payments,    "
        "    Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name AS From_this_company,    "
        "    Recipient_State AS In_the_state_of    "
        "    FROM " + str(table) + "    WHERE Physician_Full_Name <> '   '"
        " AND Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name = '" + company + "' "
        " AND Recipient_State = '" + state + "' "
        "    GROUP BY Physician_Profile_ID, Recipient_State, Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name    "
        "    ORDER BY SUM(Total_Amount_of_Payment_USDollars) DESC    "
        "    LIMIT 20;    ")
        mysql_query=mysql_query_specific_state_company

    
    #connect and perform query
    print 'User Query: ' + mysql_query
    conn = get_connection(database)
    curA=conn.cursor()
    query_result=[]
    for result in curA.execute(mysql_query,multi=True):
        if result.with_rows:
            #with the schema of physician, money, this many payments
            for line in result.fetchall():
                physician_info=[str(line[0]),str(line[1]),str(line[2])]
                if physician_info[0] != '':
                    query_result.append(physician_info)
    return query_result
        
if __name__ == '__main__':
    user='root'
    database='grand_rounds'
    table='test6' #some functions call either table or table_name
    file_location= '/home/ubuntu/cms_explorer/OP_DTL_RSRCH_PGYR2014_P06302015.csv'
    
    #get available company names and states of transactions
    companies=get_company_names(database,table)
    print 'Example company name: ', companies[0] #have to call the subarray as well to get the string
    states_abbrv, states_fullname=get_states(database,table)
    print 'Example state name: ', states_abbrv
    print 'Example full state name: ', states_fullname
    
    #append the option 'All' to companies and states for the option to not be narrowed to one state,company in search
    companies.insert(0,'All')
    states_abbrv.insert(0,'All')
    states_fullname.insert(0,'All')
    
    #query terms, 'All' is also a valid entry
    state='CO'
    company='All'
    try:
        query_result=query_database(database,table,state,company)
        if query_result != None and query_result != []:
            print 'Example query result: ', query_result
        else:
            print 'No results found'
    except:
        #bounce an error if there is no company in that state with payments for instance
        print 'ERROR ENCOUNTERED: NO RESULTS CAN BE RETURNED'
    
