import requests
import pymongo

def get_people(url):
    people = requests.get(url).json()
    characters = people["results"]
    while people["next"]:
        people = requests.get(people["next"]).json()
        characters += people["results"]
    return characters

def get_starships(url):
    starships = requests.get(url).json()
    return starships["results"]

def create_pilot_list(pilots, characters_dict):
    pilot_list = []
    for pilot in pilots:
        pilot_character = characters_dict.get(pilot)
        if pilot_character and "url" in pilot_character:
            pilot_list.append(pilot_character)
    return pilot_list


def insert_starships(starships, characters_collection, starships_collection):
    characters_dict = create_characters_dict(characters_collection)
    for starship in starships:
        pilots = create_pilot_list(starship.get("pilots", []), characters_dict)
        starship["pilots"] = [pilot["name"] for pilot in pilots]
        starships_collection.insert_one(starship)

def create_characters_dict(characters_collection):
    characters_dict = {}
    for character in characters_collection.find():
        try:
            characters_dict[character["url"]] = character
        except KeyError:
            # Handle the case where the "url" key is missing
            pass
    return characters_dict



# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["starwars"]
characters_collection = db["characters"]
starships_collection = db["starships"]

# Populate the characters collection
characters = get_people("https://swapi.dev/api/people/")
characters_collection.insert_many(characters)

# Populate the starships collection
starships = get_starships("https://swapi.dev/api/starships/")
insert_starships(starships, characters_collection, starships_collection)

# Verify the data has been inserted
print("Number of starships:", starships_collection.count_documents({}))
