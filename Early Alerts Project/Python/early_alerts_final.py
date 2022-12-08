import psycopg2
import ast
import pandas as pd
import gspread
import gspread_dataframe as gd
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials



#Open file with credentials for postgres database
credentials = open(r'c:\Users\Sam Koenig\Logins\param_dic.txt',"r")

contents = credentials.read()
param_dic = ast.literal_eval(contents)

credentials.close()

#Credentials to access google sheet
#Define scopes to push to google
scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']

##CLIENT_SECRET_FILE = '/Users/Sam Koenig/.gdrive_private/noble_truck.json'
##API_SERVICE_NAME = 'sheets'
API_VERSION = 'v4'

#Define and authorize credentials for pushing to google sheet
credentials = Credentials.from_service_account_file('/Users/Sam Koenig/.gdrive_private/noble_truck.json', scopes = scopes)

gc = gspread.authorize(credentials)


#Connect to the database
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("Connection successful")
    return conn

#Define function to grab postgres query as a pandas dataframe
def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()
    
    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df

    
# Connect to the database
conn = connect(param_dic)

#CELEBRATION QUERIES  --------------------------------------------------------------------------------------------------------------------
    #define column names for dataframe
celebrate_column_names = ["k_user", "category_id","celebrate","celebrate_url"]

    
#WELCOME SURVEY----
celebrate_welcome_survey_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\welcome_survey_celebrate.txt","r")
celebrate_welcome_survey_file = celebrate_welcome_survey_file.read()
welcome_survey_celebrate = postgresql_to_dataframe(conn, celebrate_welcome_survey_file, celebrate_column_names)

#USAGE FILE----
celebrate_usage_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\usage_celebrate.txt","r")
celebrate_usage_file = celebrate_usage_file.read()
usage_celebrate = postgresql_to_dataframe(conn, celebrate_usage_file, celebrate_column_names)

#Fellow Pulse Reminder File
celebrate_pulse_reminder_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\pulse_reminder_celebrate.txt","r")
celebrate_pulse_reminder_file = celebrate_pulse_reminder_file.read()
pulse_reminder_celebrate = postgresql_to_dataframe(conn, celebrate_pulse_reminder_file, celebrate_column_names)

#Pulse Survey Insights File
celebrate_pulse_insights = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\pulse_survey_insights_celebrate.txt","r")
celebrate_pulse_insights = celebrate_pulse_insights.read()
pulse_insights_celebrate = postgresql_to_dataframe(conn, celebrate_pulse_insights, celebrate_column_names)

#list building insights 
celebrate_list_insights = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\list_building_celebrate.txt","r")
celebrate_list_insights = celebrate_list_insights.read()
list_insights_celebrate = postgresql_to_dataframe(conn, celebrate_list_insights, celebrate_column_names)


#career interests insights 
celebrate_career_insights = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\career_interest_celebration.txt","r")
celebrate_career_insights = celebrate_career_insights.read()
career_insights_celebrate = postgresql_to_dataframe(conn, celebrate_career_insights, celebrate_column_names)


#test oti celebrate
celebrate_test = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\test_oti_celebrate.txt","r")
celebrate_test = celebrate_test.read()
test_celebrate = postgresql_to_dataframe(conn, celebrate_test, celebrate_column_names)

#mock fafsa
mock_fafsa_celebrate = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\mock_fafsa_celebrate.txt","r")
mock_fafsa_celebrate = mock_fafsa_celebrate.read()
fafsa_celebrate = postgresql_to_dataframe(conn, mock_fafsa_celebrate, celebrate_column_names)

#postsec_assignment
postsec_celebrate = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\postsec_assignment_celebration.txt","r")
postsec_celebrate = postsec_celebrate.read()
postsec_assign_celebrate = postgresql_to_dataframe(conn, mock_fafsa_celebrate, celebrate_column_names)

#pre-conference form
conference_celebrate = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\conference_form_celebration.txt","r")
conference_celebrate = conference_celebrate.read()
conference_form_celebrate = postgresql_to_dataframe(conn, conference_celebrate, celebrate_column_names)


#champion form
champion_celebrate = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Celebrations\champion_celebrate.txt","r")
champion_celebrate = champion_celebrate.read()
champion_celebrate = postgresql_to_dataframe(conn, champion_celebrate, celebrate_column_names)




#ADD ADDITIONAL CELEBRATION QUERIES HERE
#Query 3
#Query n++

#-----------------------------------------------------------------------------------------------------------------------

#UNION CELEBRATION LOGIC
celebrations_combined = pd.concat([welcome_survey_celebrate,conference_form_celebrate,champion_celebrate, postsec_assign_celebrate,fafsa_celebrate, test_celebrate, usage_celebrate, pulse_reminder_celebrate, pulse_insights_celebrate, list_insights_celebrate,career_insights_celebrate])

sh = gc.open_by_key('1CO9o-okD4zdCP6SAXCOnVUKzcHTxNOSNkdhNV7w8cqM')
priorities = sh.get_worksheet(2) #-> 0 - first sheet, 1 - second sheet etc. 
priority_order = gd.get_as_dataframe(priorities)

#join_celebrations with priority order
celebrations_ordered = celebrations_combined.merge(priority_order, left_on = 'category_id',right_on='category_id')

#order celebrations by user and then priority number
celebrations_ordered =  celebrations_ordered.sort_values(by = ['k_user','priority_number'])


#limit the number of rows to one per pd and remove multiple suggestions if they have that
celebration_final = celebrations_ordered.loc[celebrations_ordered.groupby('k_user')['priority_number'].idxmin()]



#REACH_OUT QUERIES----------------------------------------------------------------------
#----------------------------------
reach_column_names = ["k_user", "category_id","reach","reach_url"]
   
#WELCOME SURVEY 
reach_welcome_survey_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\welcome_survey_reach_out.txt","r")
reach_welcome_survey_file = reach_welcome_survey_file.read() 
# Execute the "SELECT *" query format (connection info, query, column names)
welcome_survey_reach = postgresql_to_dataframe(conn, reach_welcome_survey_file, reach_column_names)

#USAGE
reach_usage_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\usage_reach_out.txt","r")
reach_usage_file = reach_usage_file.read() 
usage_reach = postgresql_to_dataframe(conn, reach_usage_file, reach_column_names)


#Fellow Pulse Reach 
reach_pulse_reminder_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\pulse_reminder_reach.txt","r")
reach_pulse_reminder_file = reach_pulse_reminder_file.read() 
pulse_reminder_reach = postgresql_to_dataframe(conn, reach_pulse_reminder_file, reach_column_names)


#Fellow Pulse Reach 
reach_pulse_insights = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\pulse_survey_insights_reach.txt","r")
reach_pulse_insights = reach_pulse_insights.read() 
pulse_insights_reach = postgresql_to_dataframe(conn, reach_pulse_insights, reach_column_names)

#Outside of the classroom pulse check 
outside_classroom_flag = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\outside_classroom_reach_out.txt","r")
outside_classroom_flag = outside_classroom_flag.read() 
outside_classroom_reach = postgresql_to_dataframe(conn, outside_classroom_flag, reach_column_names)

#List Building Reach
reach_list_insights = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\list_building_reach_out.txt","r")
reach_list_insights = reach_list_insights.read() 
list_insights_reach = postgresql_to_dataframe(conn, reach_list_insights, reach_column_names)


#Career Interest reach 
reach_career_insights = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\career_interests_reach_out.txt","r")
reach_career_insights = reach_career_insights.read() 
career_insights_reach = postgresql_to_dataframe(conn, reach_career_insights, reach_column_names)



#Conference Form Reach Out
conference_reach = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\conference_form_reach_out.txt","r")
conference_reach = conference_reach.read() 
conference_reach_out = postgresql_to_dataframe(conn, conference_reach, reach_column_names)


#postsec Assignment reach out 
postsec_reach = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\Reach_Out\postsec_assignment_reach_out.txt","r")
postsec_reach = postsec_reach.read() 
postsec_reach_out = postgresql_to_dataframe(conn, postsec_reach, reach_column_names)



#ADD ADDITIONAL REACH OUT QUERIES HERE Below
#Query 3
#Query 4
#-----------
#Union reach out logic
reach_out_combined = pd.concat([welcome_survey_reach,postsec_reach_out,conference_reach_out, usage_reach, pulse_reminder_reach, pulse_insights_reach,outside_classroom_reach,career_insights_reach,list_insights_reach])

#join reach out with priority order on google sheet
reach_out_ordered = reach_out_combined.merge(priority_order, left_on = 'category_id',right_on='category_id')

#order reach out by user and then priority number
reach_out_ordered =  reach_out_ordered.sort_values(by = ['k_user','priority_number'])


#limit the number of rows to one per pd and remove multiple suggestions if they have that
reach_out_final = reach_out_ordered.loc[reach_out_ordered.groupby('k_user')['priority_number'].idxmin()]

#
#FORMAT FOR GOOGLE SHEET---------------------------------------------------------------------------------------
column_names = ["k_user", "first_name","subject", "intro_note","email","site_director_email", "reach_out","reach_out_url","celebrate","celebrate_url", "closing_note", "date"]
email_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\formatted_email.txt","r")
formatted_file = email_file.read() 
# Execute the "SELECT *" query format (connection info, query, column names)
formatted_df = postgresql_to_dataframe(conn, formatted_file, column_names)

formatted_df_with_celebration = formatted_df.merge(celebration_final, how='left', on = 'k_user')
formatted_df_with_reach = formatted_df_with_celebration.merge(reach_out_final, how='left', on = 'k_user')

final_dataframe = formatted_df_with_reach.drop(['k_user','reach_out','reach_out_url','celebrate_x','celebrate_url_x','category_id_x','priority_number_x','category_x','category_id_y','priority_number_y','category_y'], axis = 1) 

final_dataframe = final_dataframe[["first_name", "subject", "intro_note", "email", "site_director_email", "reach","reach_url", "celebrate_y", "celebrate_url_y","closing_note","date"]]

conn.close()



#Define what your final dataframe to push is
## ** df = welcome_survey

#PUSH TO GOOGLE SHEET ----- 
#Need to share google sheet with 'groups@noble-truck-278414.iam.gserviceaccount.com' and change the spreadsheet key below to relevant sheet
sh = gc.open_by_key('1CO9o-okD4zdCP6SAXCOnVUKzcHTxNOSNkdhNV7w8cqM')
worksheet = sh.get_worksheet(1) #-> 0 - first sheet, 1 - second sheet etc. 

#Get existing sheet number of rows so you can append
existing = gd.get_as_dataframe(worksheet)
number_of_rows = len(existing) - existing['name'].isnull().sum(axis = 0)

# APPEND DATA TO SHEET
set_with_dataframe(worksheet, final_dataframe, row = number_of_rows + 2,col = 1,include_index = False, include_column_header = False)