#!/usr/bin/env python

#151102 Flask Run App

#import connectors for mysql
import mysql.connector
from mysql.connector import errorcode

#tools for ETL and web server
import json, operator, re, os
from flask import Flask, jsonify, render_template, request, make_response
from operator import itemgetter
from functools import wraps, update_wrapper
from datetime import datetime

app = Flask(__name__)

print 'VERIFICATION: SCRIPT RUNNING'
# homepage
@app.route("/")
@app.route("/origin")
@app.route("/origin/index")
def hello():
  return render_template("index.html")

#open a template for user based queries
@app.route("/cms_explorer")
def grand_rounds():
  return render_template("cms_explorer_query.html")

@app.route("/cms_explorer", methods=['POST'])
def grand_rounds_post():
  #get information from the POST
  print 'IN grand_rounds_post function'
  try:
      state = str(request.form["state"]) # get username entered
      company = str(request.form["company"])
      print 'User selected COMPANY:',company
      print 'User selected STATE:',state
  except:
    print 'error encountered in retrieving form information for cms_explorer'
  
  #~global variables for accessing SQL table
  user='root'
  database='grand_rounds'
  table='test6' #some functions call either table or table_name

  #change the state into an abbreviation for querying mysql
  state_fullname=state #to avoid 'ALL' errors
  if state != 'All':
    states_dict = {'AK': 'Alaska','AL': 'Alabama','AR': 'Arkansas','AS': 'American Samoa','AZ': 'Arizona','CA': 'California','CO': 'Colorado','CT': 'Connecticut','DC': 'District of Columbia','DE': 'Delaware','FL': 'Florida','GA': 'Georgia','GU': 'Guam','HI': 'Hawaii','IA': 'Iowa','ID': 'Idaho','IL': 'Illinois','IN': 'Indiana','KS': 'Kansas','KY': 'Kentucky','LA': 'Louisiana','MA': 'Massachusetts','MD': 'Maryland','ME': 'Maine','MI': 'Michigan','MN': 'Minnesota','MO': 'Missouri','MP': 'Northern Mariana Islands','MS': 'Mississippi','MT': 'Montana','NA': 'National','NC': 'North Carolina','ND': 'North Dakota','NE': 'Nebraska','NH': 'New Hampshire','NJ': 'New Jersey','NM': 'New Mexico','NV': 'Nevada','NY': 'New York','OH': 'Ohio','OK': 'Oklahoma','OR': 'Oregon','PA': 'Pennsylvania','PR': 'Puerto Rico','RI': 'Rhode Island','SC': 'South Carolina','SD': 'South Dakota','TN': 'Tennessee','TX': 'Texas','UT': 'Utah','VA': 'Virginia','VI': 'Virgin Islands','VT': 'Vermont','WA': 'Washington','WI': 'Wisconsin','WV': 'West Virginia','WY': 'Wyoming'}
    state_fullname=state #rewrite here
    states_inverted_dict = dict((y,x) for x,y in states_dict.iteritems())
    state=states_inverted_dict[state] #rewriting old variable here
    print 'User selected STATE abbreviation:',state

  #function to connect to mysql server with output to user verifying that connection was made
  def get_connection(database):
    try:
      cnx = mysql.connector.connect(user = 'root',password ='',host='127.0.0.1',port='3306',database=database, buffered=True)
      print('Successfully connected to database')
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

  #MYSQL query engine
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
        mysql_query_specific_state=("SELECT Physician_Full_Name AS Physician, "
        "SUM(Total_Amount_of_Payment_USDollars) AS Received_the_following_money, "
        "count(Total_Amount_of_Payment_USDollars) AS In_this_many_payments,    "
        "Recipient_State AS In_the_state_of "
        "FROM " + str(table) + " WHERE Physician_Full_Name <> '   ' "
        "AND Recipient_State = '" + state + "' "
        "GROUP BY Physician_Profile_ID, Recipient_State "
        "ORDER BY SUM(Total_Amount_of_Payment_USDollars) DESC "
        "LIMIT 20; ")
        mysql_query=mysql_query_specific_state
    elif state=='All':
        mysql_query_specific_company=("SELECT Physician_Full_Name AS Physician, "
        "SUM(Total_Amount_of_Payment_USDollars) AS Received_the_following_money, "
        "count(Total_Amount_of_Payment_USDollars) AS In_this_many_payments, "
        "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name AS From_this_company "
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
    #mysql_query='SELECT DISTINCT Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name AS Companies FROM test6' #TEST QUERY to make sure it works
    print 'User Query: ' + mysql_query
    conn = get_connection(database)
    curA=conn.cursor()
    query_result=[]
    print 'Connected to mysql via cursor'
    for result in curA.execute(mysql_query,multi=True):
        if result.with_rows:
            #with the schema of physician, money, this many payments
            for line in result.fetchall():
                physician_info=[str(line[0]),str(line[1]),str(line[2])]
                if physician_info[0] != '':
                    query_result.append(physician_info)
    return query_result

  #generate JSON file format of results to be ported to html template
  def run_query_sql(database,table,state,company):
    try:
        results=[]
        query_result=query_database(database,table,state,company)
        if query_result != None and query_result != []:
            print 'Example query result: ', query_result[0]
            #iterate through results to pull out doctor,money and payment information
            for line in query_result:
                single_result=({"doctor": line[0], "money" : line[1], "payments":line[2]})
                results.append(single_result)
        else:
            print 'No results found'
    except:
        #bounce an error if there is no company in that state with payments for instance
        print 'ERROR ENCOUNTERED: NO RESULTS CAN BE RETURNED'
    return results

  #execute main query here and store the list of dicts for future JSON creation
  results=run_query_sql(database,table,state,company)

  #render the template with this information
  query_html_printout =  'STATE: ' + state_fullname + ' - ' 'COMPANY: ' + company #print out on the top of the page
  return render_template("cms_explorer_table.html", query_output=query_html_printout, output=results) #did not force conversion of json file



if __name__ == "__main__":
  print 'running'
  app.run(host='0.0.0.0', port = 80)
