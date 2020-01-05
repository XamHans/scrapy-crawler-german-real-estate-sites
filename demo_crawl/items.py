#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags
import re
import datetime
import logging
from babel.numbers import parse_decimal

def remove_whitespace(value):
    try:
        return value.strip().replace("€", "").replace("m²", "")
        
    except:
        return value


def remove_whitespacewg(value):
    try:
        stringer = value.strip().replace(
            "[", "").replace("]", "").replace(u'\u201e', '')
        return stringer
    except:
        print("SCHEI? QUOTES :" + str(value))
        return value


def remove_dot(value):
    return value.strip().replace("[", "").replace("]", "")

def remove_backslash(value):
    regex = re.compile(r'[\n\r\t]')
    value = regex.sub(' ', value)
    return value.strip()

def parseToNumber(value):
    if not value:
        return
    try:
        value = re.search(r'\d+(?:[.,]\d*)?', str(value)).group(0)
        parsed_miete = parse_decimal(str(value), locale='de')
        if '.' in str(parsed_miete):
            parsed_miete = str(parsed_miete).split('.')[0]
        val = int(parsed_miete)
        return val
    except Exception as e:
        logging.exception(e)
        return value

def parseZimmerOrFlache(value):
    try:
        val = int(re.search(r'\d+', str(value)).group(0))
        val = int(val)
        return val
    except Exception as e:
        logging.exception(e)
        return value

def booleanconverter(value):
    if str(value) == "NULL" or str(value) == "0":
        return 0
    else:
        return 1


def booleanwgconverter(value):
    try:
        if value == True:
            return 1
        else:
            return 0
    except:
        return 0


def haustierconverter(value):
    if str(value).upper().strip() == "NEIN":
        return 0
    else:

        return 1


def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)


def parsetoDateTime(value):

    if not value:
        return None

    DATE_FORMAT = "%d.%m.%y"
    match = ""
    try:
        if hasNumbers(value) == False:

            value = datetime.datetime.now().strftime('%Y-%m-%d')
            return value
        else:
            value = re.sub("[a-zA-Z]", "", value)
            value = value.strip()
            if "/" in value:
                match = re.sub(r'\/.*\.', '', value)

            match = re.sub(r'[^0-9.]', '', value)

            for fmt in ('%d.%m.%y', '%d.%m.%Y', '%d.%m.%Y.', '%d.%m.%y.'):
                try:
                    value = datetime.datetime.strptime(
                        match, fmt).strftime('%Y-%m-%d')
                    break
                except Exception:
                    pass
                    # print(e)

        return str(value).strip()

    except Exception as e:
        print("UNTERSCHE TIMEDATA VON URL :" + str(value))


class ImmobilieItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field(input_processor=MapCompose(
        remove_tags, remove_dot), output_processor=TakeFirst())
    kaltmiete = scrapy.Field(input_processor=MapCompose(
        remove_whitespace, parseToNumber), output_processor=TakeFirst())
    nebenkosten = scrapy.Field(input_processor=MapCompose(
        remove_whitespace,  parseToNumber), output_processor=TakeFirst())
    gesamtkosten = scrapy.Field(input_processor=MapCompose(
        remove_whitespace,  parseToNumber), output_processor=TakeFirst())
    zimmer = scrapy.Field(input_processor=MapCompose(
         remove_whitespace, parseZimmerOrFlache), output_processor=TakeFirst())
    flache = scrapy.Field(input_processor=MapCompose(
        remove_whitespace,  parseZimmerOrFlache), output_processor=TakeFirst())
    grundstuck = scrapy.Field(input_processor=MapCompose(
         remove_whitespace, parseZimmerOrFlache), output_processor=TakeFirst())
    anbieter = scrapy.Field(input_processor=MapCompose(
        remove_whitespace), output_processor=TakeFirst())
    typ = scrapy.Field(input_processor=MapCompose(
        remove_whitespace), output_processor=TakeFirst())
    bezugsfreiab = scrapy.Field(input_processor=MapCompose(
        parsetoDateTime), output_processor=TakeFirst())
    provisionsfrei = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    haustier = scrapy.Field(input_processor=MapCompose(
        haustierconverter, remove_whitespace), output_processor=TakeFirst())
    garage = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    terrasse = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    keller = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    garten = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    balkon = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    ebk = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    aufzug = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    mobliert = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())

    barriefrei = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    adresse = scrapy.Field(input_processor=MapCompose(
        remove_tags, remove_dot, remove_backslash), output_processor=TakeFirst())
    ort = scrapy.Field(input_processor=MapCompose(
        remove_tags, remove_whitespace), output_processor=TakeFirst())
    kaufen = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    haus = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    wg = scrapy.Field(input_processor=MapCompose(
        booleanconverter, remove_whitespace), output_processor=TakeFirst())
    url = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild1 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild2 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild3 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild4 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild5 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild6 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild7 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    bild8 = scrapy.Field(input_processor=MapCompose(
        remove_tags), output_processor=TakeFirst())
    stadtid = scrapy.Field(input_processor=MapCompose(
        remove_whitespace), output_processor=TakeFirst())
    stadtvid = scrapy.Field(input_processor=MapCompose(
        remove_whitespace), output_processor=TakeFirst())
    lat = scrapy.Field()
    lon = scrapy.Field()
    createdat = scrapy.Field()
    alive = scrapy.Field()
    stadtname = scrapy.Field(input_processor=MapCompose(
        remove_whitespace), output_processor=TakeFirst())


class WGItem(scrapy.Item):
    title = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    gesamtkosten = scrapy.Field()
    flache = scrapy.Field()
    anbieter = scrapy.Field()
    bezugsfreiab = scrapy.Field()
    wgsize = scrapy.Field()
    anzahlf = scrapy.Field()
    anzahlm = scrapy.Field()
    gesuchtf = scrapy.Field()
    gesuchtm = scrapy.Field()
    wgwomenonly = scrapy.Field()
    garten = scrapy.Field(input_processor=MapCompose(
        booleanwgconverter, remove_whitespace), output_processor=TakeFirst())
    balkon = scrapy.Field(input_processor=MapCompose(
        booleanwgconverter, remove_whitespace), output_processor=TakeFirst())
    kaution = scrapy.Field()
    aufzug = scrapy.Field(input_processor=MapCompose(
        booleanwgconverter, remove_whitespace), output_processor=TakeFirst())
    moebliert = scrapy.Field(input_processor=MapCompose(
        booleanwgconverter, remove_whitespace), output_processor=TakeFirst())
    lat = scrapy.Field()
    lon = scrapy.Field()
    barriefrei = scrapy.Field(input_processor=MapCompose(
        booleanwgconverter, remove_whitespace), output_processor=TakeFirst())
    adresse = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    url = scrapy.Field()
    bild1 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild2 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild3 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild4 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild5 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild6 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild7 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    bild8 = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
    stadtid = scrapy.Field()
    stadtvid = scrapy.Field(input_processor=MapCompose(
        remove_whitespacewg), output_processor=TakeFirst())
