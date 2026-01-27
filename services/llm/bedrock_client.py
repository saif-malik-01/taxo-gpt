import boto3
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

bedrock = boto3.client(
    service_name="bedrock-runtime",
    region_name="us-east-1"
)

MODEL_ID = "qwen.qwen3-next-80b-a3b"


from typing import Iterator

def call_bedrock(prompt: str) -> str:
    """
    Call Qwen model on AWS Bedrock using converse()
    """

    messages = [
        {
            "role": "user",
            "content": [
                {"text": prompt}
            ]
        }
    ]

    response = bedrock.converse(
        modelId=MODEL_ID,
        messages=messages
    )

    # ✅ QWEN converse() response format
    return response["output"]["message"]["content"][0]["text"]


def call_bedrock_stream(prompt: str) -> Iterator[str]:
    """
    Call Qwen model on AWS Bedrock using converse_stream()
    """
    messages = [
        {
            "role": "user",
            "content": [
                {"text": prompt}
            ]
        }
    ]

    response = bedrock.converse_stream(
        modelId=MODEL_ID,
        messages=messages
    )

    stream = response.get("stream")
    if stream:
        for event in stream:
            if "contentBlockDelta" in event:
                yield event["contentBlockDelta"]["delta"]["text"]
