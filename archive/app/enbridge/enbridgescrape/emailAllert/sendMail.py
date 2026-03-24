from azure.communication.email import EmailClient
import base64


def sendReport():
    try:

        client = EmailClient.from_connection_string(connection_string)

        with open("<path-to-your-attachment>", "rb") as file:
            file_bytes_b64 = base64.b64encode(file.read())

        message = {
            "senderAddress": "DoNotReply@9adde4a4-40b3-48c0-aeb4-a91fb3fd6dfb.azurecomm.net",
            "recipients": {
                "to": [
                    {"address": "bart.burk@gasfundies.com"},
                    {"address": "sai.jagadeesh@gasfundies.com"},
                    {"address": "shawn.mathai@gasfundies.com"}
                ]
            },
            "content": {
                "subject": "Test Email",
                "plainText": """Hi Guys,\n
				\n
				This is a testing mail for mail service""",
                "html": """
				<html>
					<body>
						<h1>
							Hi Guys,<br/>
							<br/>
							This is a testing mail for mail service
						</h1>
					</body>
				</html>"""
            },
            "attachments": [
                {
                    "name": "<your-attachment-name>",
                    "contentType": "<your-attachment-mime-type>",
                    "contentInBase64": file_bytes_b64.decode()
                }
            ]

        }

        poller = client.begin_send(message)
        result = poller.result()
        print("Message sent: ", result.message_id)

    except Exception as ex:
        print(ex)


main()


attachment_filepath = "path/to/your/file.pdf"  # Replace with your file path
with open(attachment_filepath, "rb") as f:
    file_content_bytes = f.read()
encoded_content = base64.b64encode(file_content_bytes).decode('utf-8')

attachment = EmailAttachment(
    name=os.path.basename(attachment_filepath),
    content_type="application/pdf",  # Replace with the correct MIME type
    content_bytes_base64=encoded_content
)
