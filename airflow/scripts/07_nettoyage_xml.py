"""
AniData Lab - XML Cleaning Script
===================================
Entry : data/anime_3.xml
Output: data/anime_xml_gold.csv
"""

import xml.etree.ElementTree as ET
import pandas


# ============================================
# CONFIG
# ============================================
INPUT_FILE  = "data/anime_3.xml"
OUTPUT_FILE = "data/anime_xml_gold.csv"

UNKNOWN_VALUES = ["Unknown", "UNKNOWN", "unknown", "N/A", "-", ""]


# ============================================
# UTILITIES
# ============================================
def parse_xml(filepath: str) -> list[dict]:
    """Parse XML file and return a list of anime records."""
    tree = ET.parse(filepath)
    root = tree.getroot()

    records = []
    for anime in root.findall("anime"):
        genres = [g.text for g in anime.findall("genres/genre")]
        record = {
            "anime_id": anime.findtext("anime_id"),
            "name":     anime.findtext("name"),
            "genres":   ", ".join(genres) if genres else None,
            "type":     anime.findtext("type"),
            "episodes": anime.findtext("episodes"),
            "rating":   anime.findtext("rating"),
            "members":  anime.findtext("members"),
            "year":     anime.findtext("year"),
            "studio":   anime.findtext("studio"),
            "status":   anime.findtext("status"),
        }
        records.append(record)

    return records


def replace_unknowns(df: pandas.DataFrame) -> pandas.DataFrame:
    """Replace unknown string variants with NaN."""
    return df.replace(UNKNOWN_VALUES, pandas.NA)


def fix_numeric_types(df: pandas.DataFrame) -> pandas.DataFrame:
    """Convert numeric columns to correct types."""
    df["anime_id"] = pandas.to_numeric(df["anime_id"], errors="coerce")
    df["episodes"] = pandas.to_numeric(df["episodes"], errors="coerce")
    df["rating"]   = pandas.to_numeric(df["rating"],   errors="coerce")
    df["members"]  = pandas.to_numeric(df["members"],  errors="coerce")
    df["year"]     = pandas.to_numeric(df["year"],     errors="coerce")
    return df


def validate_ratings(df: pandas.DataFrame) -> pandas.DataFrame:
    """Set ratings out of [1, 10] to NaN."""
    mask = df["rating"].notna() & ((df["rating"] < 1) | (df["rating"] > 10))
    df.loc[mask, "rating"] = pandas.NA
    return df


def add_features(df: pandas.DataFrame) -> pandas.DataFrame:
    """Add business metrics."""
    df["n_genres"]   = df["genres"].str.split(", ").str.len()
    df["main_genre"] = df["genres"].str.split(", ").str[0]
    df["is_airing"]  = df["status"].apply(lambda x: True if x == "Airing" else False)
    return df


# ============================================
# MAIN
# ============================================
def clean(input_file: str, output_file: str):
    """Run all cleaning steps and export gold CSV."""

    # STEP 1 - Parse XML
    print("Step 1 - Parsing XML...")
    records = parse_xml(input_file)
    df = pandas.DataFrame(records)
    print(f"  {len(df)} records loaded, {len(df.columns)} columns")

    # STEP 2 - Replace unknowns
    print("Step 2 - Replacing unknowns...")
    df = replace_unknowns(df)

    # STEP 3 - Fix types
    print("Step 3 - Fixing types...")
    df = fix_numeric_types(df)

    # STEP 4 - Validate ratings
    print("Step 4 - Validating ratings...")
    df = validate_ratings(df)

    # STEP 5 - Remove duplicates
    print("Step 5 - Removing duplicates...")
    rows_before = len(df)
    df = df.drop_duplicates(subset=["anime_id"], keep="first")
    print(f"  {rows_before - len(df)} duplicates removed")

    # STEP 6 - Feature engineering
    print("Step 6 - Adding features...")
    df = add_features(df)

    # REPORT
    print("\n=== CLEANING REPORT ===")
    print(f"  Rows    : {len(df)}")
    print(f"  Columns : {len(df.columns)}")
    print(f"  Missing values:\n{df.isnull().sum()[df.isnull().sum() > 0]}")
    print(f"  Types:\n{df.dtypes}")

    # EXPORT
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"\nStep 7 - Exported to: {output_file}")
    print("Done!")


clean(INPUT_FILE, OUTPUT_FILE)