from mimesis import Person, TimestampFormat, Gender
from mimesis import Field, Fieldset, Schema
from mimesis import Locale
from pprint import pprint
from mimesis import random
import time
from datetime import datetime
import pymongo

random.global_seed = 0xFF

field = Field(Locale.EN)
fieldset = Fieldset(Locale.EN)

user_schema_definition = lambda: {
    "pk": field("increment"),
    "uid": field("uuid"),
    "name": field("person.full_name"),
    "address": {
        "street": field("address"),
        "city": field("address.city"),
        "state": field("address.state"),
        "zip_code": field("address.zip_code"),
    },
    "phone": field("phone_number"),
    "gender": field("person.gender"),
    "date_of_birth": field("person.birthdate", max_year=2002).isoformat(),
    "created": field("timestamp", fmt=TimestampFormat.POSIX),
    "email": field("person.email"),
}


userschema = Schema(schema=user_schema_definition, iterations=1000)
user_records = userschema.create()
def generate_users():
    return user_records

def select_uid(random, **kwargs):
    return random.choice(user_records).get("uid")

random_uid = Field()
field.register_handler("random_uid", select_uid)

event_schema_definition = lambda: {
    "pk": field("increment"),
    "uid": field("random_uid"),
    "event": "asdfasd",
    "timestamp": datetime.now().isoformat(),
}

def generate_event():
    eventschema = Schema(schema=event_schema_definition, iterations=1)
    return eventschema.create()[0]

if __name__ == "__main__":
    user_records = generate_users()

    client = pymongo.MongoClient("10.29.0.4")
    db = client.test_db
    users = db.users
    result = users.insert_many(user_records)

    print(result.inserted_ids)
    print(f"Inserted {len(user_records)} documents")

    events = db.events
    while True:
        event_record = generate_event()
        pprint(event_record)
        result = events.insert_one(event_record)
        print(result.inserted_id)
        print(f"Inserted event for user {event_record.get('uid')}")
        time.sleep(15)
