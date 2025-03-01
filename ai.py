import datetime
import json

from openai import OpenAI
from openai.types.beta import Thread
from openai.types.beta.threads import Message

ASSISTANT_KEY = 'categorizer'

def make_thread(
    client: OpenAI,
) -> Thread:
    thread = client.beta.threads.create(
        messages=[
            {
                "role": "user",
                "content": f"All upcoming requests will be one or more articles, json-formatted list of strings with 1 or more elements. I want to have list of tags for each article. Each tag must be a string without any space, so use underscores or camelCase, but the same format for all tags. Good to have 5 tags per each article, maximum is 10. Reuse tags as much as possible (if applicable). For the next message I don't want to have any output, just analyse it and get tags. If article text contains only link - it does not have any tags (put null). All tags must be in english.",
            },
            {
                "role": "user",
                "content": "For all upcoming messages provide list of lists of tags. Request will be a json, where key is id of an article, value - text of that article. Response must be json as well, where key is id of an article, value - tags that you choose for an article.",
            }
        ]
    )
    return thread



def submit_articles(
    client: OpenAI, thread_id: str, assistant_id: str, articles: dict[str, str]
) -> dict[str, list[str]]:
    client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=json.dumps(articles),
    )

    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread_id,
        assistant_id=assistant_id,
    )
    messages = client.beta.threads.messages.list(
        thread_id=thread_id, run_id=run.id,
    )
    return json.loads(messages.data[0].content[0].text.value)



def get_assistant(client: OpenAI):
    assistants = client.beta.assistants.list()
    for ass in assistants.data:
        if ASSISTANT_KEY in ass.name:
            return ass
    return None

def create_assistant(client: OpenAI):
    model = "gpt-4o-mini"
    assistant_version = datetime.datetime.now(datetime.UTC)
    return client.beta.assistants.create(
        name=f"{ASSISTANT_KEY} {model}-{assistant_version}",
        response_format={"type": "json_object"},
        instructions="""
        I'll provide several articles, unrelated and not bound.
        I want you to provide list of tags for each article.
        Better have as less tags (topics) as possible but as long as numbers of articles is potentially infinitive it may not be possible.
""",
        model=model,
    )
