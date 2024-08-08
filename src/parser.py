import gzip
import os
from lxml import etree


class ParserUtils:
    @staticmethod
    def find_text(element, text):
        return [
            e.findtext(text)
            for e in element
            if e.findtext(text) and e.findtext(text) != ""
        ]

    @staticmethod
    def get_text(element, text):
        return [e.get(text) for e in element if e.get(text)]

    @staticmethod
    def parse_urls(element):
        url_element = element.find("urls")
        if url_element is None:
            return []
        return [u.text.strip() for u in url_element if u.text is not None]

    @staticmethod
    def parse_genre_styles(element):
        styles_element = element.find("styles")
        genres_element = element.find("genres")
        styles = [
            s.text
            for s in (styles_element if styles_element is not None else [])
            if s.text is not None
        ]
        genres = [
            g.text
            for g in (genres_element if genres_element is not None else [])
            if g.text is not None
        ]
        return {"genre": genres, "style": styles}

    @staticmethod
    def parse_aliases(element):
        aliases_element = element.find("aliases")
        if aliases_element is None:
            return []
        return [alias.text for alias in aliases_element if alias.text is not None]

    @staticmethod
    def parse_videos(element, element_type="release"):
        video_element_parent = element.find("videos")
        if video_element_parent is None:
            return {}
        video_element = video_element_parent.findall("video")
        video_urls = [v.get("src") for v in video_element]
        video_duration = [d.get("duration") for d in video_element]
        if element_type == "master":
            video_description = ParserUtils.find_text(
                video_element_parent, "description"
            )
            return {
                "urls": video_urls,
                "duration": video_duration,
                "description": video_description,
            }
        return {"urls": video_urls, "duration": video_duration}


class BaseParser:
    def __init__(self, file_path, sample=False) -> None:
        self.file_path = file_path
        self.check_file_exists()
        self.sample = sample

    def check_file_exists(self):
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"File does not exist: {self.file_path}")

    def iterate_and_decompress_xml(self):
        with gzip.open(self.file_path, "rb") as f:
            context = etree.iterparse(f, events=("end",), tag=self.tag)
            for i, (_, element) in enumerate(context):
                if element is not None:
                    yield element
                    if element.tag not in ["label", "sublabels"]:
                        element.clear()
                    while element.getprevious() is not None:
                        del element.getparent()[0]
                    if self.sample:
                        if i >= 50_000:
                            break

    def parse_file(self):
        for element in self.iterate_and_decompress_xml():
            parsed_data = self.parse_elements(element)
            if parsed_data:
                yield parsed_data

    def parse_elements(self, element):
        raise NotImplementedError("Subclasses should implement this method")


class LabelParser(BaseParser):
    def __init__(self, file_path, sample=False) -> None:
        super().__init__(file_path, sample)
        self.tag = "label"

    def parse_sub_labels(self, element, parent_label_id):
        sub_label_element = element.findall("sublabels/label")
        sub_labels = []
        for sub_label in sub_label_element:
            if sub_label.get("id") and sub_label.text:
                sublabel_data = {
                    "id": sub_label.get("id"),
                    "label": sub_label.text,
                    "parent_label_id": parent_label_id,
                }
                sub_labels.append(sublabel_data)

        return sub_labels

    def parse_label_info(self, element):
        contact_info = element.findtext("contactinfo")
        profile = element.findtext("profile")
        data_quality = element.findtext("data_quality")
        return {
            "contact_info": contact_info,
            "profile": profile,
            "data_quality": data_quality,
        }

    def parse_elements(self, element):
        label_id = element.findtext("id")
        label_name = element.findtext("name")
        if label_id is None:
            return []

        label_data = {
            "id": label_id,
            "label": label_name,
            "parent_label_id": None,
        }

        return {
            **label_data,
            **self.parse_label_info(element),
            "urls": ParserUtils.parse_urls(element),
            "sublabel": self.parse_sub_labels(element, label_id),
        }


class ArtistParser(BaseParser):
    def __init__(self, file_path, sample=False) -> None:
        super().__init__(file_path, sample)
        self.tag = "artist"

    def parse_name_variations(self, element):
        name_var_element = element.find("namevariations")
        if name_var_element is None:
            return []
        return [
            n_var.text.strip() for n_var in name_var_element if n_var.text is not None
        ]

    def parse_artist(self, element):
        artist_id = element.findtext("id")
        if artist_id is None:
            return {}
        return {
            "id": artist_id,
            "artist": element.findtext("name"),
            "real_name": element.findtext("realname"),
            "profile": (
                element.findtext("profile").strip()
                if element.findtext("profile")
                else element.findtext("profile")
            ),
            "data_quality": element.findtext("data_quality"),
        }

    def parse_elements(self, element):
        return {
            **self.parse_artist(element),
            "urls": ParserUtils.parse_urls(element),
            "aliases": ParserUtils.parse_aliases(element),
            "name_variations": self.parse_name_variations(element),
        }


class ReleasesParser(BaseParser):
    def __init__(self, file_path, sample=False) -> None:
        super().__init__(file_path, sample)
        self.tag = "release"

    def parse_release_extra_artist(self, element):
        if element is None:
            return {}

        extra_artist_element = element.find("extraartists")
        extra_artist = ParserUtils.find_text(extra_artist_element, "name")
        anv = ParserUtils.find_text(extra_artist_element, "anv")
        roles = ParserUtils.find_text(extra_artist_element, "role")
        extra_artist_id = ParserUtils.find_text(extra_artist_element, "id")
        extra_artist_tracks = ParserUtils.find_text(extra_artist_element, "tracks")
        return {
            "extra_artist": extra_artist,
            "extra_artist_id": extra_artist_id,
            "extra_artist_tracks": extra_artist_tracks,
            "extra_artist_anv": anv,
            "extra_artist_roles": roles,
        }

    def parse_release_artists(self, element):
        if element is None:
            return {}
        artist_element = element.find("artists")
        artist = ParserUtils.find_text(artist_element, "name")
        artist_id = ParserUtils.find_text(artist_element, "id")
        join_text = "".join(ParserUtils.find_text(artist_element, "join"))
        extra_artist = self.parse_release_extra_artist(element)
        return {
            "artist": artist,
            "artist_id": artist_id,
            "join_text": join_text,
            **extra_artist,
        }

    def parse_release_label(self, element):
        label_element = element.find("labels")
        if label_element is None:
            return {}
        label_name = ParserUtils.get_text(label_element, "name")[0]
        label_id = ParserUtils.get_text(label_element, "id")[0]
        catno = (
            ParserUtils.get_text(label_element, "catno")[0]
            if ParserUtils.get_text(label_element, "catno")
            else ""
        )
        return {"label_id": label_id, "label_name": label_name, "catno": catno}

    def parse_formats(self, element):
        formats_element = element.find("formats")
        if formats_element is None:
            return {}
        format_name = ParserUtils.get_text(element.find("formats"), "name")[0]
        descriptions = [desc.text for desc in formats_element.findall(".//description")]
        quantity = ParserUtils.get_text(element.find("formats"), "qty")[0]
        return {
            "format": format_name,
            "format_description": ",".join(descriptions),
            "quantity": quantity,
        }

    def parse_release(self, element):
        release_id = element.get("id")
        title = element.findtext("title")
        release_date = element.findtext("released")
        notes = element.findtext("notes")
        master_id = element.findtext("master_id")
        country = element.findtext("country")
        is_master_release = (
            element.find("master_id").get("is_main_release")
            if element.find("master_id") is not None
            else None
        )
        return {
            "id": release_id,
            "title": title,
            "master_id": master_id,
            "release_date": release_date,
            "notes": notes,
            "country": country,
            "is_master_release": is_master_release,
        }

    def parse_release_company(self, element):
        companies_element = element.find("companies")
        if companies_element is None:
            return {}
        company_elements = [c for c in companies_element.findall("company")]
        company_ids = ParserUtils.find_text(company_elements, "id")
        company_name = ParserUtils.find_text(company_elements, "name")
        company_role = ParserUtils.find_text(company_elements, "entity_type_name")

        return {
            "company_id": company_ids,
            "company_names": company_name,
            "company_roles": company_role,
        }

    def parse_tracks(self, element):
        tracklist_element = element.find("tracklist")
        track_elements = tracklist_element.findall("track")
        track_position = [p.findtext("position") for p in track_elements]
        track_title = [p.findtext("title") for p in track_elements]
        track_duration = [p.findtext("duration") for p in track_elements]
        return {
            "track_position": track_position,
            "track_title": track_title,
            "track_duration": track_duration,
        }

    def parse_elements(self, element):
        return {
            **self.parse_release(element),
            **self.parse_release_artists(element),
            **self.parse_release_label(element),
            **self.parse_formats(element),
            **ParserUtils.parse_genre_styles(element),
            **self.parse_tracks(element),
            **ParserUtils.parse_videos(element, element_type="release"),
            **self.parse_release_company(element),
        }


class MasterParser(BaseParser):
    def __init__(self, file_path, sample=False) -> None:
        super().__init__(file_path, sample)
        self.tag = "master"

    def parse_master_artist(self, element):
        if element is None:
            return {}
        artist_element = element.find("artists")
        artist_id = ParserUtils.find_text(artist_element, "id")
        artist = ParserUtils.find_text(artist_element, "name")
        join_text = ",".join(ParserUtils.find_text(artist_element, "join"))
        # role = ParserUtils.find_text(artist_element, "role")
        # tracks = ",".join(ParserUtils.find_text(artist_element, "tracks"))
        anv = ParserUtils.find_text(artist_element, "anv")
        return {
            "artist_id": artist_id,
            "artist": artist,
            "join_text": join_text,
            "anv": anv,
        }

    def parse_master_release(self, element):
        if element is None:
            return {}
        master_id = element.get("id")
        year = element.findtext("year")
        title = element.findtext("title")
        data_quality = element.findtext("data_quality")
        title = element.findtext("title")
        return {
            "id": master_id,
            "year": year,
            "title": title,
            "data_quality": data_quality,
        }

    def parse_elements(self, element):
        return {
            **self.parse_master_release(element),
            **self.parse_master_artist(element),
            **ParserUtils.parse_videos(element, element_type="master"),
            **ParserUtils.parse_genre_styles(element),
        }
