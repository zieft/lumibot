import os

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

gpt_client = OpenAI(
    api_key=os.environ.get('OPENAI_API_KEY')
)
