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


from typing import Iterator, List, Optional

def call_bedrock(prompt: str, system_prompts: Optional[List[str]] = None, temperature: float = 0.0) -> str:
    """
    Call Qwen model on AWS Bedrock using converse() with error handling
    Args:
        prompt: The user message content
        system_prompts: Optional list of system prompt strings
        temperature: Inference temperature (0.0 for deterministic)
    """

    messages = [
        {
            "role": "user",
            "content": [
                {"text": prompt}
            ]
        }
    ]
    
    # Prepare system block if provided
    system_block = []
    if system_prompts:
        for sp in system_prompts:
            system_block.append({"text": sp})

    inference_config = {
        "temperature": temperature,
        "maxTokens": 4096,
        "topP": 0.9
    }

    try:
        kwargs = {
            "modelId": MODEL_ID,
            "messages": messages,
            "inferenceConfig": inference_config
        }
        if system_block:
            kwargs["system"] = system_block

        response = bedrock.converse(**kwargs)
        return response["output"]["message"]["content"][0]["text"]
    except Exception as e:
        logger.error(f"Bedrock call failed: {str(e)}")
        return "NONE" # Return NONE to signal failure or no facts


def call_bedrock_stream(prompt: str, system_prompts: Optional[List[str]] = None, temperature: float = 0.0) -> Iterator[str]:
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

    # Prepare system block if provided
    system_block = []
    if system_prompts:
        for sp in system_prompts:
            system_block.append({"text": sp})

    inference_config = {
        "temperature": temperature,
        "maxTokens": 4096,
        "topP": 0.9
    }

    try:
        kwargs = {
            "modelId": MODEL_ID,
            "messages": messages,
            "inferenceConfig": inference_config
        }
        if system_block:
            kwargs["system"] = system_block

        response = bedrock.converse_stream(**kwargs)

        stream = response.get("stream")
        if stream:
            for event in stream:
                if "contentBlockDelta" in event:
                    yield event["contentBlockDelta"]["delta"]["text"]
    except Exception as e:
        logger.error(f"Bedrock stream failed: {str(e)}")
        yield "\n[Error: Connection to AI lost. Please try again or check your parameters.]"
