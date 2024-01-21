
# Email Alerts Automation
**Context** - The purpose of this file is to transform data from an AWS database using PostgresQL, push these transformed dataframes to a google sheet using [gspread_dataframe](https, ://gspread-dataframe.readthedocs.io/en/latest/), and then create the javascript code using google apps script to allow a practitioner to send out emails to an intended recipients within the google sheets UI. 

*Ideally, this system would be automated, but in it's MVP format, it's important for our team to be able to review the contents of this data to QA test this in the field before sending it out. Google sheets meet the criteria*

Requirements: 

 - Step 1 - Ensure that the directory information is correct for the
   postgres database 
  - Step 2 - Create a google developer account with a
   service account that we can share our google sheet with. I found the steps outlined in this [medium article](https://medium.com/@jb.ranchana/write-and-append-dataframes-to-google-sheets-in-python-f62479460cf0) helpful 
   - Step 3 - Ensure that the directory information is correct in
   FY23_Early_Alerts_Final.py for the Client Secret File (line 25) and
   Credentials (line 31) so that the code is referencing the correct
   location.  
   - Step 4 - Create a google apps script file in the google
   sheet of interest and drop the "email_automation.js" and the
   "teacher_email.html" file into the apps script 
 
 - Step 5 - Ensure that
   your service provider created in your Google developer console is shared with the google sheet where we are sending the emails of interest.

 
