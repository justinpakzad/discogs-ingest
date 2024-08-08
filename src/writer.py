import csv


class BaseWriter:
    def __init__(self, file_name=None) -> None:
        self.headers = []
        self.file_name = file_name
        self.file = None
        self.writer = None
        self.batch_size = 25_000
        self.buffer = []

    def open_file(self):
        if self.file_name:
            try:
                self.file = open(self.file_name, mode="w", newline="")
                self.writer = csv.DictWriter(self.file, fieldnames=self.headers)
                self.writer.writeheader()
            except Exception as e:
                raise Exception(f"Failed to open/write to {self.file_name}.csv: {e}")

    def write_row(self, row):
        self.buffer.append(row)
        if self.writer and len(self.buffer) >= self.batch_size:
            self.writer.writerows(self.buffer)
            self.buffer.clear()

    def close_file(self):
        if self.buffer:
            self.writer.writerows(self.buffer)
            self.buffer.clear()
        if self.file:
            self.file.close()


class SimpleWriter(BaseWriter):
    def write_rows(self, rows):
        self.open_file()
        for row in rows:
            data = {k: row[k] for k in self.headers if k in row}
            self.write_row(data)
        self.close_file()


class NestedWriter(BaseWriter):
    def write_rows(self, rows):
        self.open_file()
        for row in rows:
            for sub_item in self.get_sub_items(row):
                self.write_row(sub_item)
        self.close_file()

    def get_sub_items(self, row):
        raise NotImplemented


class LabelWriter(SimpleWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["id", "label", "contact_info", "data_quality"]


class LabelUrlWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["label_id", "url"]

    def get_sub_items(self, row):
        return [
            {"label_id": row.get("id"), "url": url.strip()}
            for url in row.get("urls", [])
        ]


class SubLabelWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["id", "label", "parent_label_id"]

    def get_sub_items(self, row):
        return [
            {
                "id": sub.get("id"),
                "label": sub.get("label").strip(),
                "parent_label_id": sub.get("parent_label_id"),
            }
            for sub in row.get("sublabel")
        ]


class ArtistWriter(SimpleWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["id", "artist", "real_name", "profile", "data_quality"]


class ArtistAliasWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = [
            "artist_id",
            "alias",
        ]

    def get_sub_items(self, row):
        return [
            {"artist_id": row.get("id"), "alias": alias.strip()}
            for alias in row.get("aliases", [])
        ]


class ArtistUrlWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["artist_id", "url"]

    def get_sub_items(self, row):
        return [
            {"artist_id": row.get("id"), "url": url.strip()}
            for url in row.get("urls", [])
        ]


class ArtistNameVariationWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = [
            "artist_id",
            "name_var",
        ]

    def get_sub_items(self, row):
        return [
            {"artist_id": row.get("id"), "name_var": name_var.strip()}
            for name_var in row.get("name_variations", "")
        ]


class ReleaseWriter(SimpleWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = [
            "id",
            "title",
            "master_id",
            "release_date",
            "notes",
            "country",
            "is_master_release",
            "format",
            "format_description",
            "label_name",
            "label_id",
            "catno",
        ]


class ReleaseGenreWrite(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["release_id", "genre"]

    def get_sub_items(self, row):
        return [
            {"release_id": row.get("id"), "genre": genre} for genre in row.get("genre")
        ]


class ReleaseTracksWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = [
            "release_id",
            "title",
            "position",
            "duration",
            "duration_seconds",
        ]

    def get_sub_items(self, row):
        return [
            {
                "release_id": row.get("id"),
                "title": trck_title,
                "duration": trck_duration,
                "position": trck_position,
                "duration_seconds": duration_secs,
            }
            for trck_title, trck_duration, trck_position, duration_secs in zip(
                row.get("track_title", []),
                row.get("track_duration", []),
                row.get("track_position", []),
                row.get("duration", []),
            )
        ]


class ReleaseArtistWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["release_id", "artist_id"]

    def get_sub_items(self, row):
        return [
            {"release_id": row.get("id"), "artist_id": artist_id}
            for artist_id in row.get("artist_id", "")
        ]


class ReleaseExtraArtistWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["release_id", "artist_id", "role"]

    def get_sub_items(self, row):
        return [
            {"release_id": row.get("id"), "artist_id": artist_id, "role": role}
            for artist_id, role in zip(
                row.get("extra_artist_id", []), row.get("extra_artist_roles")
            )
        ]


class ReleaseStyleWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["release_id", "style"]

    def get_sub_items(self, row):
        return [
            {"release_id": row.get("id"), "style": style.strip()}
            for style in row.get("style", "")
        ]


class ReleaseCompanyWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["release_id", "company_id", "company_name", "role"]

    def get_sub_items(self, row):
        return [
            {
                "release_id": row.get("id"),
                "company_id": company_id,
                "company_name": company_name,
                "role": company_role,
            }
            for company_id, company_name, company_role in zip(
                row.get("company_id", []),
                row.get("company_names", []),
                row.get("company_roles", []),
            )
        ]


class ReleaseVideoWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["release_id", "url"]

    def get_sub_items(self, row):
        return [
            {"release_id": row.get("id"), "url": url} for url in row.get("urls", "")
        ]


class MasterWriter(SimpleWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["id", "title", "year", "data_quality"]


class MasterArtistWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["master_id", "artist_id"]

    def get_sub_items(self, row):
        return [
            {"master_id": row.get("id"), "artist_id": artist_id}
            for artist_id in row.get("artist_id", [])
        ]


class MasterVideoWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["master_id", "url", "duration", "description"]

    def get_sub_items(self, row):
        return [
            {
                "master_id": row.get("id"),
                "url": url.strip(),
                "duration": duration,
                "description": description.strip(),
            }
            for url, duration, description in zip(
                row.get("urls", []), row.get("duration", []), row.get("description", [])
            )
        ]


class MasterGenreWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["master_id", "style"]

    def get_sub_items(self, row):
        return [
            {"master_id": row.get("id"), "style": style.strip()}
            for style in row.get("genre")
        ]


class MasterStylesWriter(NestedWriter):
    def __init__(self, file_name=None) -> None:
        super().__init__(file_name)
        self.headers = ["master_id", "style"]

    def get_sub_items(self, row):
        return [
            {"master_id": row.get("id"), "style": style.strip()}
            for style in row.get("style")
        ]
