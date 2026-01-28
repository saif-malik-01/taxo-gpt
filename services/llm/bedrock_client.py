from botocore.config import Config
import boto3
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# Config for better reliability and performance
bedrock_config = Config(
    region_name="us-east-1",
    read_timeout=120,      # Increased to 2 minutes
    connect_timeout=30,
    retries={
        "max_attempts": 3,
        "mode": "adaptive" # More robust for rate limits / transients
    }
)

bedrock = boto3.client(
    service_name="bedrock-runtime",
    config=bedrock_config
)

MODEL_ID = "qwen.qwen3-next-80b-a3b"


from typing import Iterator

def call_bedrock(prompt: str) -> str:
    """
    Call Qwen model on AWS Bedrock using converse() with error handling
    """

    messages = [
        {
            "role": "user",
            "content": [
                {"text": prompt}
            ]
        }
    ]

    try:
        response = bedrock.converse(
            modelId=MODEL_ID,
            messages=messages
        )
        return response["output"]["message"]["content"][0]["text"]
    except Exception as e:
        logger.error(f"Bedrock call failed: {str(e)}")
        return "NONE" # Return NONE to signal failure or no facts


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
