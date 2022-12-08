/*Add the early alert button to the google sheet*/ 
function onOpen() 
{
    var ss = SpreadsheetApp.getActive();
      var items = [
        {name: 'Send Email', functionName: 'doApprove'},

      ];
      ss.addMenu('Early Alert', items);
}

/*CODE STARTS FOR ACTUAL EMAIL SEND*/
/*function to alert whether the user wants to send email or not*/ 
function doApprove()
{
   var cell = SpreadsheetApp.getActiveSheet().getActiveCell();
   var row = cell.getRow();

   var alert = getAlertFromRow(row); 

   var ui = SpreadsheetApp.getUi();
   var response = ui.alert('Send Email to '+alert.pd_name+'?', ui.ButtonSet.YES_NO);

   if(response == ui.Button.YES)
   {
     handleApproval(row, alert);
   }
}


/*function to define the variables from the sheet*/
function getAlertFromRow(row)
{
  var values = SpreadsheetApp.getActiveSheet().getRange(row, 1,row,15).getValues();
  var rec = values[0];
  var alert = 
      {
        date: rec[0],
        pd_email: rec[1],
        sd_email: rec[2],
        subject: rec[3],
        pd_name: rec[4],
        intro_note: rec[5],
        advising_alert: rec[6].substring(0,rec[6].lastIndexOf(" ")),
        advising_link_word: rec[6].substring(rec[6].lastIndexOf(" ")),
        advising_link: rec[7],
        milestone: rec[8],
        milestone_lesson_url: rec[9],
        milestone_instructions: rec[10],
        milestone_alert: rec[11].substring(0,rec[11].lastIndexOf(" ")),
        milestone_alert_word: rec[11].substring(rec[11].lastIndexOf(" ")),
        milestone_checklist_url: rec[12],
        closing_note: rec[13],
        key: rec[14],
        status: rec[15],
        date_sent:rec[16]
      };

    
   return alert;
}


/*function to send notification upon clicking "yes" and timestamp date of email send */
function handleApproval(row, alert)
{
  var templ = HtmlService.createTemplateFromFile('teacher_email');
  templ.alert = alert;
  
  var message = templ.evaluate().setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL).getContent();


  if(alert.status != 'Email Sent') {
      MailApp.sendEmail({
        name: 'OneGoal Notification',
        to: alert.pd_email,
        cc: alert.sd_email,
        subject: alert.subject,
        htmlBody: message
      })  
  }
  else {
      SpreadsheetApp.getUi().alert('Email Already Sent To This User');
  }

/*set status of email after approval to sent, and mark the date that it was sent*/
  SpreadsheetApp.getActiveSheet().getRange(row, 16).setValue('Email Sent');
  SpreadsheetApp.getActiveSheet().getRange(row, 17).setValue(Utilities.formatDate(new Date(), "GMT+1", "MM/dd/yyyy"));


} 