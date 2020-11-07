import random
from random import randint
from uuid import uuid4

from faker import Faker

from data_generator.utils import save_json


def generate_persons(gen, _range):
    person_tmp = []
    uuid_tmp = []
    for i in range(_range):
        person, uuid = gen.generate_person()
        person_tmp.append(person)
        uuid_tmp.append(uuid)
    return person_tmp, uuid_tmp


class Generator:
    def __init__(self):
        self.faker = Faker()

    def generate_person(self) -> (dict, str):
        profile = self.faker.simple_profile()
        person_uuid = str(uuid4())
        profile["uuid"] = person_uuid
        return profile, person_uuid

    def generate_person_action(self, uuid) -> dict:
        person_activity = dict()
        person_activity["uuid"] = uuid
        person_activity["uri_path"] = self.faker.uri_path()
        person_activity["date_time"] = self.faker.date_time_this_year()
        return person_activity


def run_generator(logging, person_count, count_activities_range, folder, filename):
    gen = Generator()
    person_list, uuid_list = generate_persons(gen, person_count)
    logging.info("The profiles were generated")
    activities = [gen.generate_person_action(uuid) for uuid in uuid_list for _ in
                  range(randint(*count_activities_range))]
    logging.info("The persons' activities were generated")

    full_json = person_list + activities

    random.shuffle(full_json)
    save_json(full_json, folder, filename)
    logging.info("The json with persons and activities was saved")

