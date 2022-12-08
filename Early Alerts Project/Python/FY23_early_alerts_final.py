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


#START Advisng alerts here #
advising_column_names = ["k_course", "advising_alert","advising_url"]
advising_alert_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\FY23_advising_alert.txt","r")
advising_alert_file = advising_alert_file.read()
advising_alert_file = postgresql_to_dataframe(conn, advising_alert_file, advising_column_names)


#Start Milestone Alert Here#
milestone_column_names = ["k_course", "cohort_year","course_type","milestone_name","milestone_lesson_url","milestone_instructions", "milestone_alert", "milestone_url"]
milestone_alert_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\FY23_milestone_alerts.txt","r")
milestone_alert_file = milestone_alert_file.read()
milestone_alert_file = postgresql_to_dataframe(conn, milestone_alert_file, milestone_column_names)


#Formatted email file load
column_names = ["k_course","date", "pd_email","sd_email", "subject","pd_name","intro_note", "test12","test12","test","test1", "test2", "test3","test4","closing note","key"]
email_file = open(r"C:\Users\Sam Koenig\Desktop\Projects\Early Alerts Project\SQL\formatted_email.txt","r")
formatted_file = email_file.read()
formatted_df = postgresql_to_dataframe(conn, formatted_file, column_names)


#merge the milestone data and the advising data
formatted_df_with_milestone = formatted_df.merge(milestone_alert_file, how='left', on = 'k_course')
formatted_df_combined = formatted_df_with_milestone.merge(advising_alert_file, how='left', on = 'k_course')
#formatted_df_with_celebration = formatted_df.merge(celebration_final, how='left', on = 'k_user')





#formatted_df_with_reach = formatted_df_with_celebration.merge(reach_out_final, how='left', on = 'k_user')
final_dataframe = formatted_df_combined[["date", "pd_email","sd_email", "subject","pd_name","intro_note", "advising_alert","advising_url","milestone_name","milestone_lesson_url", "milestone_instructions", "milestone_alert","milestone_url","closing note","key"]]

conn.close()
print(final_dataframe)


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
