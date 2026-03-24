message = {
    "content": {
        "subject": "str",  # Subject of the email message. Required.
        "html": "str",  # Optional. Html version of the email message.
        "plainText": "str"  # Optional. Plain text version of the email
    },
    "recipients": {
        "to": [
            {
                "address": "str",  # Email address. Required.
                "displayName": "str"  # Optional. Email display name.
            }
        ],
        "bcc": [
            {
                "address": "str",  # Email address. Required.
                "displayName": "str"  # Optional. Email display name.
            }
        ],
        "cc": [
            {
                "address": "str",  # Email address. Required.
                "displayName": "str"  # Optional. Email display name.
            }
        ]
    },
    # Sender email address from a verified domain. Required.
    "senderAddress": "str",
    "attachments": [
        {
            "name": "str"  # Name of the attachment. Required.
            # MIME type of the content being attached. Required.
            "contentType": "str",
            # Base64 encoded contents of the attachment. Required.
            "contentInBase64": "str",
            # Unique identifier (CID) to reference an inline attachment. Optional
            "contentId": "str"
        }
    ],
    # Optional. Indicates whether user engagement tracking should be disabled for this request if the resource-level user engagement tracking setting was already enabled in the control plane.
    "userEngagementTrackingDisabled": bool,
    "headers": {
        "str": "str"  # Optional. Custom email headers to be passed.
    },
    "replyTo": [
        {
            "address": "str",  # Email address. Required.
            "displayName": "str"  # Optional. Email display name.
        }
    ]
}

response = {
    "id": "str",  # The unique id of the operation. Uses a UUID. Required.
    "status": "str",  # Status of operation. Required. Known values are:
    # "NotStarted", "Running", "Succeeded", and "Failed".
    "error": {
        "additionalInfo": [
            {
                "info": {},  # Optional. The additional info.
                "type": "str"  # Optional. The additional info type.
            }
        ],
        "code": "str",  # Optional. The error code.
        "details": [
            ...
        ],
        "message": "str",  # Optional. The error message.
        "target": "str"  # Optional. The error target.
    }
}
