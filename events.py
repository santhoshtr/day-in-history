from time import sleep
from typing import Optional

import requests
from bs4 import BeautifulSoup
from datasets import Dataset
from pydantic import BaseModel
from tqdm import tqdm

language = "en"


class Event(BaseModel):
    year: int
    month: str
    day: int
    event_description: str
    reference: Optional[str] = None

    def __str__(self):
        return f"{self.date}: {self.description}"


def parse_year(year):
    if "BCE" in year:
        year = year.replace("BCE", "").strip()
        year = -int(year)
    elif "BC" in year:
        year = year.replace("BC", "").strip()
        year = -int(year)
    elif "or" in year or "OR" in year:
        year = year.split("or")[0].strip()
    elif "AD" in year:
        year = year.replace("AD", "").strip()
        year = int(year)
    elif "CE" in year:
        year = year.replace("CE", "").strip()
        year = int(year)
    else:
        print(f"\nUnknown year format {year}")
        return None
    return year


def get_events(month, day):
    events = []
    url = f"https://{language}.wikipedia.org/api/rest_v1/page/html/{month}_{day}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch {month}/{day}: {e}")
        return events

    soup = BeautifulSoup(response.text, "html.parser")
    event_els = soup.select("[data-mw-section-id='1'] li")

    for event_el in event_els:
        no_entity = False
        no_link = False
        if "about" in event_el.attrs:
            continue
        year = event_el.text.split("–")[0].strip()

        try:
            year = parse_year(year) if not year.isdigit() else int(year)
            if year is None:
                continue
        except Exception as e:
            print(f"Failed to parse year {year}: {e}")
            continue

        if isinstance(year, str):
            print(f"Skipping {day} {month} {year}")
            continue

        # remove the year from the event description html
        link = event_el.find("a[rel='mw:WikiLink']")
        if link:
            link.decompose()
        else:
            no_link = True

        for link in event_el.select("a[rel='mw:WikiLink']"):
            # change links to absolute links
            if link.get("href").startswith("./"):
                link["href"] = (
                    f"https://{language}.wikipedia.org/wiki/{link['href'].replace('./', '')}"
                )

        entity = event_el.select("span[typeof='mw:Entity']")
        if entity:
            entity[0].decompose()
        else:
            no_entity = True

        ref_el = event_el.select("sup[typeof='mw:Extension/ref']")
        reference = None
        if ref_el:
            ref_note_id = ref_el[0].attrs.get("id").replace("_ref", "_note")
            ref_note_el = soup.select("li[id='" + ref_note_id + "'] .mw-reference-text")
            if ref_note_el:
                style_el = ref_note_el[0].select("style")
                if style_el:
                    style_el[0].decompose()
                link_el = ref_note_el[0].select("link")
                if link_el:
                    link_el[0].decompose()
                reference = str(ref_note_el[0])
            ref_el[0].decompose()

        event_description = event_el.decode_contents().split(f"{year}")[-1].strip()
        if event_description.startswith("</a>"):
            event_description = event_description.replace("</a>", "", 1)
        if no_entity:
            event_description = event_description.split("–")[-1]
            event_description = event_description.split("-")[-1]

        events.append(
            Event(
                year=year,
                month=month,
                day=day,
                event_description=event_description,
                reference=reference,
            )
        )
    return events


# loop all dates in  a year
months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
days = range(1, 32)


def gen():
    for month in months:
        for day in tqdm(days, desc=month, initial=1, total=31):
            events = get_events(month, day)
            for event in events:
                yield event.dict()
        sleep(10)


ds = Dataset.from_generator(gen)
ds.to_parquet(f"day_in_history.en.parquet")
