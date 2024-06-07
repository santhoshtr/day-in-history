from time import sleep
from typing import Optional

import requests
from bs4 import BeautifulSoup
from datasets import Dataset
from pydantic import BaseModel
from tqdm import tqdm
import argparse

supported_languages = ["en", "ml"]


class Event(BaseModel):
    year: int
    month: int
    day: int
    event_description: str
    reference: Optional[str] = None

    def __str__(self):
        return f"{self.date}: {self.description}"


bc_patterns = ["BCE", "BC", "ബി.സി.", "ബിസി", "ബി.സി.ഇ", "ക്രി.മു"]
ad_patterns = ["AD", "CE", "ക്രി.ശേ."]


def parse_year(year):
    if year.isdigit():
        return int(year)
    if any(bc in year for bc in bc_patterns):
        for pattern in bc_patterns:
            year = year.replace(pattern, "")
        year = year.strip()
        year = -int(year)
    elif any(ad in year for ad in ad_patterns):
        for pattern in ad_patterns:
            year = year.replace(pattern, "")
        year = year.strip()
        year = int(year)

    else:
        # Try some other patterns
        if "-" in year:
            year = year.split("-")[0].strip()
            return parse_year(year)
        elif " " in year:
            year = year.split(" ")[0].strip()
            return parse_year(year)

        print(f"\nUnknown year format {year}")
        return None
    return year


def get_events(language, month, day):
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
        if "about" in event_el.attrs:
            continue
        year = event_el.text.split("–")[0].strip()

        try:
            year = parse_year(year)
            if year is None:
                continue
        except Exception as e:
            print(f"Failed to parse year {year}: {e}")
            continue

        if isinstance(year, str):
            print(f"Skipping {day} {month} {year}")
            continue

        for link in event_el.select("a[rel='mw:WikiLink']"):
            # change links to absolute links
            if link.get("href").startswith("./"):
                link["href"] = (
                    f"https://{language}.wikipedia.org/wiki/{link['href'].replace('./', '')}"
                )

        entity = event_el.select("span[typeof='mw:Entity']")
        if entity:
            event_description = (
                event_el.decode_contents().split(f"span>", 1)[-1].strip()
            )
        else:
            event_description = event_el.decode_contents().split("–", 1)[-1]
            event_description = event_description.split("-", 1)[-1]

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

        events.append(
            Event(
                year=year,
                month=months.get(language).index(month) + 1,
                day=day,
                event_description=event_description,
                reference=reference,
            )
        )
    return events

months = {
    "ml": [
        "ജനുവരി",
        "ഫെബ്രുവരി",
        "മാർച്ച്",
        "ഏപ്രിൽ",
        "മേയ്",
        "ജൂൺ",
        "ജൂലൈ",
        "ഓഗസ്റ്റ്",
        "സെപ്റ്റംബർ",
        "ഒക്ടോബർ",
        "നവംബർ",
        "ഡിസംബർ",
    ],
    "en": [
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
    ],
}
days = range(1, 32)


def gen(language):
    for month in months.get(language):
        for day in tqdm(days, desc=month, initial=1, total=31):
            events = get_events(language, month, day)
            for event in events:
                yield event.dict()
        sleep(1)


parser = argparse.ArgumentParser()
parser.add_argument(
    "--language",
    help="Language code for Wikipedia",
    default="en",
    choices=supported_languages,
)
args = parser.parse_args()

language = args.language
ds = Dataset.from_generator(gen, gen_kwargs={"language": language})
ds.to_parquet(f"day_in_history.{language}.parquet")
