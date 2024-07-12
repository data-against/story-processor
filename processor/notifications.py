import logging
import os
import smtplib
import ssl
import tempfile
import time
from typing import List

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from processor import get_email_config, is_email_configured, is_slack_configured

logger = logging.getLogger(__name__)


def send_email(recipients: List[str], subject: str, message: str) -> bool:
    """
    Send an email to a sysadmin or something.
    :param recipients: email addresses to send to
    :param subject:
    :param message: plaintext message
    :return: boolean success
    """
    if not is_email_configured():
        logger.warning(
            "Ignoring cowardly attempt send email to {} when no email configured".format(
                recipients
            )
        )
        return False
    email_config = get_email_config()
    logger.info(
        "Sending email from={} to={}".format(email_config["from_address"], recipients)
    )
    msg = "Subject: {}\n\n{}".format(subject, message)
    context = ssl.create_default_context()
    with smtplib.SMTP(email_config["address"], email_config["port"]) as server:
        server.starttls(context=context)
        server.login(email_config["user_name"], email_config["password"])
        for email_address in recipients:
            server.sendmail(
                email_config["from_address"], email_address, msg.encode("utf8")
            )
    logger.info("  sent")
    return True


def upload_to_slack(
    channel_id: str, bot_key: str, source: str, subject: str, file_path
) -> bool:
    if not is_slack_configured():
        logger.warning("Ignoring cowardly attempt post slack without tokens")
        return False
    client = WebClient(token=bot_key)
    try:
        filename = source + "-" + time.strftime("%Y%m%d-%H%M%S")
        response = client.files_upload_v2(
            channel=channel_id,
            file=file_path,
            title=filename.upper(),
            initial_comment=subject,
        )
        if response["ok"]:
            return True
        else:
            return False
    except SlackApiError as e:
        print(f"Slack API error: {e}")
        return False


def send_slack_msg(
    channel_id, bot_key, data_source: str, subject: str, message: str
) -> bool:
    if not is_slack_configured():
        logger.warning("Ignoring cowardly attempt post slack without tokens")
        return False

    header = f"{subject.upper()}"
    formatted_message = f"{header}\n\n{message}"
    channel = channel_id

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False
    ) as temp_file:
        temp_file.write(formatted_message)
        temp_file.flush()
        temp_file_name = temp_file.name
        success = upload_to_slack(channel, bot_key, data_source, header, temp_file_name)
        if success:
            logger.info("Slack message sent successfully")
        else:
            logger.error("Failed to send Slack message")

    if temp_file_name is not None:
        os.remove(temp_file_name)

    return success
