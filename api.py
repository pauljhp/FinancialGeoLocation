import googlemaps as gmap
from typing import (Sequence, Optional, Union, Any)
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import iter_by_chunk

LOCATION_TYPE_FILTER = ["parking",
    "place_of_trainsit",

]


class PlacesQuery:
    def __init__(self, apikey: str):
        self.apikey = apikey
        self.gmaps = gmap.Client(key=apikey)

    def geo_encode(self, place_id: str, **kwargs) -> pd.DataFrame:
        """get geocode information from placeid"""
        details = gmap.geocoding.geocode(self.gmaps,
            place_id=place_id, 
            **kwargs
        )[0]\
        .get('address_components')
        df = pd.concat([pd.Series(d).to_frame().T for d in details])
        df = df.explode("types")
        return df


    def get_country(self, place_id, **kwargs):
        """get country name based on place id"""
        encoding = self.geo_encode(place_id, **kwargs)
        country = encoding.query("types=='country'").loc[0, "long_name"]
        return country


    def place_lookup(self, 
        keyword: str, 
        limit: int=240, 
        radius=1e7, 
        location_type_filter: Sequence[str]=LOCATION_TYPE_FILTER,
        location_type_filter_out: bool=True,
        **kwargs):
        """lookup for a location based on keyword
        :param loation_type_filter: keywords in the location type field to be 
            filtered
        :param location_type_filter_out: if true, filters away the specified 
            keywords, otherswise only rows containing the keywords will be kept
        """
        res = gmap.places.places(self.gmaps, query=keyword, **kwargs)
        results = res.get('results')
        # i = 0
        while res.get("next_page_token") and len(results) < limit:
            new_url = f'https://maps.googleapis.com/maps/api/place/textsearch/json'
            placetoken = res.get("next_page_token")
            params = dict(placetoken=placetoken, 
                key=self.apikey, 
                query=keyword, 
                radius=radius,
                **kwargs
            )
            res = requests.get(new_url, params=params).json()
            results += res.get('results')
            # i += 1
        # print(i, placetoken)
        df = pd.concat([pd.Series(d).to_frame().T for d in results])
        df.loc[:, "country"] = df.place_id.apply(self.get_country)
        filter = eval("&".join([f"(df.types.str.contains('{k}'))" 
            for k in location_type_filter]))
        df = df.loc[~filter] if location_type_filter_out else df.loc[filter]
        return df
    
    @classmethod
    def search_place(cls, apikey: str, keyword: str, limit: int=240, **kwargs):
        return cls(apikey).place_lookup(keyword=keyword, limit=limit, **kwargs)

    def batch_lookup(self, 
        keywords: Sequence[str], 
        limit: int=240, 
        radius=1e7, 
        location_type_filter: Sequence[str]=LOCATION_TYPE_FILTER,
        location_type_filter_out: bool=True,
        max_workers: int=16,
        **kwargs):
        """concurrently lookup for a sequence of keywords"""
        res = []
        if len(keywords) <= max_workers:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(
                    self.place_lookup,
                    keyword=k,
                    limit=limit, 
                    radius=radius, 
                    location_type_filter=location_type_filter,
                    location_type_filter_out=location_type_filter_out,
                    **kwargs
                    )
                for k in keywords
                ]
                for future in as_completed(futures):
                    res += [future.result()]
        else:
            for chunk in iter_by_chunk(keywords, max_workers):
                res += self.batch_lookup(chunk, limit, radius, 
                    location_type_filter, location_type_filter_out, 
                    max_workers, **kwargs)
        return res

    @classmethod
    def batch_place_lookup(cls, 
        apikey: str,
        keywords: Sequence[str], 
        limit: int=240, 
        radius=1e7, 
        location_type_filter: Sequence[str]=LOCATION_TYPE_FILTER,
        location_type_filter_out: bool=True,
        max_workers: int=16,
        **kwargs):
        return cls(apikey=apikey).batch_lookup(
            keywords, limit, radius, location_type_filter, 
            location_type_filter_out, max_workers, **kwargs)
