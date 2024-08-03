from writer import *


def setup_writers(csv_path=None):
    writers = {
        "artist_writer": ArtistWriter(file_name=f"{csv_path}/artist.csv"),
        "artist_alias_writer": ArtistAliasWriter(
            file_name=f"{csv_path}/artist_alias.csv",
        ),
        "artist_url_writer": ArtistUrlWriter(file_name=f"{csv_path}/artist_url.csv"),
        "artist_name_var_writer": ArtistNameVariationWriter(
            file_name=f"{csv_path}/artist_name_variation.csv",
        ),
        "label_writer": LabelWriter(
            file_name=f"{csv_path}/label.csv",
        ),
        "label_url_writer": LabelUrlWriter(
            file_name=f"{csv_path}/label_url.csv",
        ),
        "label_sub_writer": SubLabelWriter(
            file_name=f"{csv_path}/sub_label.csv",
        ),
        "release_writer": ReleaseWriter(
            file_name=f"{csv_path}/release.csv",
        ),
        "release_tracks_writer": ReleaseTracksWriter(
            file_name=f"{csv_path}/release_tracks.csv",
        ),
        "release_artist_writer": ReleaseArtistWriter(
            file_name=f"{csv_path}/release_artist.csv",
        ),
        "release_extra_artist_writer": ReleaseExtraArtistWriter(
            file_name=f"{csv_path}/release_extra_artist.csv",
        ),
        "release_style_writer": ReleaseStyleWriter(
            file_name=f"{csv_path}/release_style.csv",
        ),
        "release_company_writer": ReleaseCompanyWriter(
            file_name=f"{csv_path}/release_company.csv",
        ),
        "release_video_writer": ReleaseVideoWriter(
            file_name=f"{csv_path}/release_video.csv",
        ),
        "master_writer": MasterWriter(file_name=f"{csv_path}/master.csv"),
        "master_video_writer": MasterVideoWriter(
            file_name=f"{csv_path}/master_video.csv",
        ),
        "master_styles_writer": MasterStyles(file_name=f"{csv_path}/master_style.csv"),
        "master_artist_writer": MasterArtistWriter(
            file_name=f"{csv_path}/master_artist.csv",
        ),
    }
    return writers
