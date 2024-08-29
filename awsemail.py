import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate
import awsemailpandas  # Import the awsmailpandas script

# Loading variables from .env file
load_dotenv('.env')

# Start a session using your credentials
session = boto3.Session(
    aws_access_key_id=os.getenv('ID'),
    aws_secret_access_key=os.getenv('key'),
    region_name='ap-south-1'  # Change to your region
)

# Create an SES client
ses_client = session.client('ses')

# Email details
sender = "ruthicksreeclusterx@gmail.com"
recipient = "ggnanesh@clustrex.com"
subject = "Amazon SES with attachment"
body_text = ("Amazon SES Test (Python)\r\n"
             "This email was sent with Amazon SES using the "
             "AWS SDK for Python (Boto3) along with an attachment.")
body_html = """<html>
<head></head>
<body>
  <h1>Amazon SES Test (Python)</h1>
  <p>This email was sent with
    <a href='https://aws.amazon.com/ses/'>Amazon SES</a> using the
    <a href='https://boto3.amazonaws.com/v1/documentation/api/latest/index.html'>AWS SDK for Python (Boto3)</a>.</p>
</body>
</html>
"""            

# Call the awsmailpandas script to fetch data and save it as a CSV file
attachment_file_path = awsemailpandas.fetch_and_save_data()
#print(attachment_file_path)

# Create a multipart/mixed parent container
msg = MIMEMultipart('mixed')
msg['From'] = sender
msg['To'] = recipient
msg['Subject'] = subject
msg['Date'] = formatdate(localtime=True)

# Create a multipart/alternative child container
msg_body = MIMEMultipart('alternative')

# Attach the text and HTML versions of the email content
textpart = MIMEText(body_text, 'plain')
htmlpart = MIMEText(body_html, 'html')

msg_body.attach(textpart)
msg_body.attach(htmlpart)

# Attach the multipart/alternative container to the multipart/mixed container
msg.attach(msg_body)

# Add attachment
if attachment_file_path:
    with open(attachment_file_path, 'rb') as attachment_file:
        attachment = MIMEApplication(attachment_file.read())
        attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_file_path))
        msg.attach(attachment)

# Try to send the email
try:
    # Send the email via Amazon SES
    response = ses_client.send_raw_email(
        Source=sender,
        Destinations=[recipient],
        RawMessage={
            'Data': msg.as_string(),
        }
    )
# Display an error if something goes wrong.    
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print("Email sent! Message ID:", response['MessageId'])