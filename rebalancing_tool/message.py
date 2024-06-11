import requests
import json


def send_teams_message(webhook_url, message):
    headers = {"Content-Type": "application/json"}

    # Split message into lines
    lines = message.split('\n')

    # Group lines into parts of 1950 characters or less
    message_parts = []
    current_part = ''
    for line in lines:
        if len(current_part) + len(line) > 1950:
            message_parts.append(current_part)
            current_part = line
        else:
            current_part += '\n' + line
    message_parts.append(current_part)  # Add the last part

    for part in message_parts:
        payload = {"content": f"```{part}```"}  # for TEAMS should be {"text": part}
        response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

        if response.status_code == 204: # for TEAMS should be 200
            print("Message part sent successfully.")
        else:
            print(f"Failed to send message part. Status code: {response.status_code}")
            print(response.text)

    return response.status_code