from peewee import OperationalError
from models import Business
from models import Review
from models import User
from models import Checkin
from models import Neighborhood
from models import Category
from models import Tip
import json
import decimal
from datetime import datetime
import pymysql


def iterate_file(model_name, shortcircuit=True, status_frequency=10000):
    i = 0
    jsonfilename = f"json/yelp_training_set_{model_name.lower()}.json"

    with open(jsonfilename, encoding='utf-8') as jfile:
        for line in jfile:
            i += 1
            try:
                yield json.loads(line)
            #except UnicodeDecodeException:
            #    print(f"Error decoding line {i} in file {jsonfilename}")
            #    continue  # Skip this line and continue with the next iteration
            except json.JSONDecodeError:
                print(f"Error parsing JSON on line {i} in file {jsonfilename}")
                continue  # Skip this line and continue with the next iteration

            if i % status_frequency == 0:
                print(f"Status >>> {jsonfilename}: {i}")
                if shortcircuit and i == 10:
                    break  # Use 'break' instead of raising StopIteration


def save_businesses():
    businesses_to_insert = []
    for bdata in iterate_file("business", shortcircuit=False):
        business = Business(
            business_id=bdata['business_id'],
            name=bdata['name'],
            full_address=bdata['address'],
            city = bdata['city'],
            state = bdata['state'],
            latitude = bdata['latitude'],
            longitude = bdata['longitude'],
            stars = decimal.Decimal(bdata.get('stars', 0)),
            review_count = int(bdata['review_count']),
            is_open = 1 if bdata['is_open'] == 1 else False,
            attributes = bdata['attributes'],
            categories = bdata['categories']
        )
        businesses_to_insert.append(business)

        if len(businesses_to_insert) >= 1000:  # adjust the number based on memory and performance
            Business.bulk_create(businesses_to_insert)
            businesses_to_insert = []

    # Insert any remaining businesses
    if businesses_to_insert:
        Business.bulk_create(businesses_to_insert)


def save_categories(business_id, cat_jarray):
    if cat_jarray is None:
        return
    for name in cat_jarray:
        category = Category()
        category.business_id = business_id
        category.category_name = name
        category.save()


def save_neighborhoods(business_id, hood_jarray):
    for hood in hood_jarray:
        neighborhood = Neighborhood()
        neighborhood.business_id = business_id
        neighborhood.neighborhood_name = hood
        neighborhood.save()


def save_reviews():
    for rdata in iterate_file("review", shortcircuit=False):
        rev = Review()
        rev.business_id = rdata['business_id']
        rev.user_id = rdata['user_id']
        rev.stars = int(rdata.get('stars', 0))
        rev.text = rdata['text']
        rev.date = datetime.strptime(rdata['date'], "%Y-%m-%d %H:%M:%S")
        # rev.useful_votes = int(rdata['votes']['useful'])
        # rev.funny_votes = int(rdata['votes']['funny'])
        # rev.cool_votes = int(rdata['votes']['cool'])
        rev.save()


def save_users():
    for udata in iterate_file("user", shortcircuit=False):
        user = User()
        user.user_id = udata['user_id']
        user.name = udata['name']
        user.review_count = int(udata['review_count'])
        user.average_stars = decimal.Decimal(udata.get('average_stars', 0))
        user.useful_votes = int(udata['votes']['useful'])
        user.funny_votes = int(udata['votes']['funny'])
        user.cool_votes = int(udata['votes']['cool'])
        user.save()


def save_checkins():
    for cdata in iterate_file("checkin", shortcircuit=False):
        checkin = Checkin()
        checkin.business_id = cdata['business_id']
        for day in range(7):
            for hour in range(24):
                number = int(cdata['checkin_info'].get("%s-%s" % (hour, day), 0))
                if day is 0:
                    checkin.sunday_count += number
                elif day is 1:
                    checkin.monday_count += number
                elif day is 2:
                    checkin.tuesday_count += number
                elif day is 3:
                    checkin.wednesday_count += number
                elif day is 4:
                    checkin.thursday_count += number
                elif day is 5:
                    checkin.friday_count += number
                elif day is 6:
                    checkin.saturday_count += number
                    checkin.save()


def save_tips():
    for tdata in iterate_file("tip", shortcircuit=True):
        tip = Tip()
        tip.business_id = tdata['business_id']
        tip.text = tdata['text']
        tip.user_id = tdata['user_id']
        tip.date = datetime.strptime(tdata['date'], "%Y-%m-%d")
        tip.likes = int(tdata['likes'])
        tip.save()


def reset_database():
    tables = (Business, Review, User, Checkin, Neighborhood, Category, Tip,)
    for table in tables:
        # Nuke the Tables
        try:
            table.drop_table()
        except OperationalError:
            pass
        # Create the Tables
        try:
            table.create_table()
        except OperationalError:
            pass


if __name__ == "__main__":
    reset_database()

    save_businesses()
    #save_users()
    #save_checkins()
    save_reviews()
    # save_tips()

