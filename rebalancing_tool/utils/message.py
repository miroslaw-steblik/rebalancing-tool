import requests
import json


# def send_discord_message(webhook_url, message):
#     headers = {"Content-Type": "application/json"}

#     # Split message into lines
#     lines = message.split('\n')

#     # Group lines into parts of 1950 characters or less
#     message_parts = []
#     current_part = ''
#     for line in lines:
#         if len(current_part) + len(line) > 1950:
#             message_parts.append(current_part)
#             current_part = line
#         else:
#             current_part += '\n' + line
#     message_parts.append(current_part)  # Add the last part

#     for part in message_parts:
#         payload = {"content": f"```{part}```"}  # for TEAMS should be {"text": part}
#         response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

#         if response.status_code == 204: # for TEAMS should be 200
#             print("Message part sent successfully.")
#         else:
#             print(f"Failed to send message part. Status code: {response.status_code}")
#             print(response.text)

#     return response.status_code

# def send_teams_message(webhook_url, message):
#     headers = {"Content-Type": "application/json"}

#     # Split message into lines
#     lines = message.split('\n')

#     # Group lines into parts of 1950 characters or less
#     message_parts = []
#     current_part = ''
#     for line in lines:
#         if len(current_part) + len(line) > 1950:
#             message_parts.append(current_part)
#             current_part = line
#         else:
#             current_part += '\n' + line
#     message_parts.append(current_part)  # Add the last part

#     for part in message_parts:
#         payload = {"content": f"```{part}```"}  # for TEAMS should be {"text": part}
#         response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

#         if response.status_code == 204: # for TEAMS should be 200
#             print("Message part sent successfully.")
#         else:
#             print(f"Failed to send message part. Status code: {response.status_code}")
#             print(response.text)

#     return response.status_code


# import smtplib
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText

# def send_email(subject, message, from_email, to_email, smtp_server, smtp_port, smtp_username, smtp_password):
#     msg = MIMEMultipart()
#     msg['From'] = from_email
#     msg['To'] = to_email
#     msg['Subject'] = subject
#     msg.attach(MIMEText(message, 'plain'))

#     server = smtplib.SMTP(smtp_server, smtp_port)
#     server.starttls()
#     server.login(smtp_username, smtp_password)
#     text = msg.as_string()
#     server.sendmail(from_email, to_email, text)
#     server.quit()

# # Replace these with your own values
# smtp_server = '127.0.0.1'
# smtp_port = 1025
# smtp_username = 'fatherofgreywolf@protonmail.com'
# smtp_password = 'PRO#mirste1209'
# from_email = 'fatherofgreywolf@protonmail.com'
# to_email = 'miroslawsteblik@gmail.com'
# subject = 'Test Email'
# message = 'This is a test email.'

# send_email(subject, message, from_email, to_email, smtp_server, smtp_port, smtp_username, smtp_password)

def send_message_to_teams(message, webhook_url):
    headers = {"Content-Type": "application/json"}
    payload = {
        "content": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.0",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": message,
                            "wrap": True
                        }
                    ]
                }
            }
        ]
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
    return response.status_code