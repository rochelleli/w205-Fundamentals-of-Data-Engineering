#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())
    
@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('swords', default_event)
    return "This is the default response!\n"

# Swords
@app.route("/petrock")
def petrock():
    purchase_petrock_event = {'event_type': 'petrock'}
    log_to_kafka('swords', purchase_petrock_event)
    return "Petrock Sword Purchased!\n"

@app.route("/splinter")
def splinter():
    purchase_splinter_event = {'event_type': 'splinter'}
    log_to_kafka('swords', purchase_splinter_event)
    return "Splinter Sword Purchased!\n"

@app.route("/excalibur")
def excalibur():
    purchase_excalibur_event = {'event_type': 'excalibur'}
    log_to_kafka('swords', purchase_excalibur_event)
    return "Excalibur Sword Purchased!\n"

@app.route("/dragonfire")
def dragonfire():
    purchase_dragonfire_event = {'event_type': 'dragonfire'}
    log_to_kafka('swords', purchase_dragonfire_event)
    return "Dragonfire Sword Purchased!\n"

# Shields
@app.route("/bubble")
def bubble():
    purchase_bubble_event = {'event_type': 'bubble'}
    log_to_kafka('shields', purchase_bubble_event)
    return "Bubble Shield Purchased!\n"

@app.route("/captainamerica")
def captainamerica():
    purchase_captainamerica_event = {'event_type': 'captainamerica'}
    log_to_kafka('shields', purchase_captainamerica_event)
    return "Captain America's Shield Purchased!\n"

@app.route("/aegis")
def aegis():
    purchase_aegis_event = {'event_type': 'aegis'}
    log_to_kafka('shields', purchase_aegis_event)
    return "Aegis Shield Purchased!\n"

# Potions
@app.route("/superman")
def superman():
    purchase_superman_event = {'event_type': 'superman'}
    log_to_kafka('potions', purchase_superman_event)
    return "Superman Potion Purchased!\n"

@app.route("/ironman")
def ironman():
    purchase_ironman_event = {'event_type': 'ironman'}
    log_to_kafka('potions', purchase_ironman_event)
    return "Ironman Potion Purchased!\n"

@app.route("/beauty")
def beauty():
    purchase_beauty_event = {'event_type': 'beauty'}
    log_to_kafka('potions', purchase_beauty_event)
    return "Beauty Potion Purchased!\n"

# Guilds
@app.route("/foreveryoung")
def foreveryoung():
    join_foreveryoung_event = {'event_type': 'foreveryoung'}
    log_to_kafka('guilds', join_foreveryoung_event)
    return "Joined Forever Young Guild!\n"

@app.route("/gonewiththewin")
def gonewiththewin():
    join_gonewiththewin_event = {'event_type': 'gonewiththewin'}
    log_to_kafka('guilds', join_gonewiththewin_event)
    return "Joined Gone with the Win Guild!\n"

